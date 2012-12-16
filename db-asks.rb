require 'active_record'
require 'json'
load './ask-human.rb'
ARb = ActiveRecord::Base
ARb.establish_connection(:adapter => "sqlite3", :database => "turk.db")

class DataType < ARb
  has_many :question_types
end
class QuestionType < ARb
  belongs_to :data_type
  #serialize :gold_standards, JSON
  has_many :questions
end
class Question < ARb
  belongs_to :question_type
  #serialize :question, JSON
  has_many :asks
end
class Ask < ARb
  belongs_to :question
  has_many :answers
  has_one :shipped_ask
end
class Hit < ARb
  has_many :assignments
  has_many :shipped_asks
  has_many :asks, :through => :shipped_asks
  attr_accessible :id
end
class ShippedAsk < ARb
  belongs_to :hit
  belongs_to :ask
  attr_accessible :ask_id, :hit_id
end
class Assignment < ARb
  belongs_to :hit
  #serialize :assignment, JSON
  has_many :answers
  attr_accessible :id, :worker_id, :assignment
end
class Answer < ARb
  belongs_to :ask
  belongs_to :assignment
end
class OldAndFullBatch < ARb
end

def newest_batch
  maybe_question_type = OldAndFullBatch.first
  maybe_question_type.nil? ? [] : maybe_question_type.ask_ids.scan(/\d+/).map(&:to_i)
end

def ship!(ask_ids)
  # we assume that all the questions have the same question_type.
  prng = lambda {|s| Digest::MD5.hexdigest(ask_ids.sort.join + s) }
  prng_a = lambda {|a| prng[a.join] }
  asks = Ask.find_all_by_id(ask_ids, :include => :question).to_a
  if asks.first.question.question_type.data_type.dtype == "radio"
    inquiries = asks.map {|a|
      q = JSON.parse(a.question.question)
      answers = q[1..-1].each_with_index.map {|ans,i| {:id => i.to_s, :text => ans }}
      {:id => a.id.to_s, :text => q[0], :answers => answers}
    }
    qt = asks.first.question.question_type
    golds = JSON.parse(qt.gold_standards).sort_by(&prng_a)[0, qt.max_gold_standards]
    gold_inquiries = golds.map {|g|
      answers = g[1..-1].each_with_index.map {|ans,i| {:id => i.to_s, :text => ans }}
      {:text => g.first, :answers => answers.sort_by(&prng_a), :correct => '0'}
    }
    batch = inquiries + gold_inquiries
    hit_id = simple_ask_questions(batch, :radio, qt.instructions, qt.samples)
    ARb.transaction {
      Hit.create!(:id => hit_id)
      Asks.each {|a| ShippedAsk.create!(:ask_id => a.id, :hit_id => hit_id) }
    }
  end
  nil
end

def consume!()
  max_gs_failure = lambda {|hit_id| Hit.find(hit_id).asks.first.question.question_type.gs_failure_rate/100.0 }
  hit_id_to_samples = lambda {|hit_id| Hit.find(hit_id).asks.first.question.question_type.samples }
  consume_hit = lambda {|hit_id, assignment|
    ARb.transaction {
      mgsfr = max_gs_failure[hit_id]
      assignment.each {|a|
        Assignment.create!(:id => a[:id], :worker_id => a[:worker_id], :assignment => JSON.dump(a[:assignment]))
        if gs_failure_rate(a) < mgsfr
          a[:assignment].each {|ask_id, q_answer_idx|
            ask = Ask.find(ask_id.to_i)
            answer = JSON.parse(ask.question.question)[q_answer_idx.to_i + 1]
            Answer.create!(:assignment_id => a[:id], :ask_id => ask_id.to_i, :answer => answer)
          }
        end
      }
    }
  }
  consume_hits(consume_hit, hit_id_to_samples, max_gs_failure)
end

def fill_in_putget_ask(instructions, question, type, gold_standards, nonce, samples, max_gold_standards, gs_failure_rate)
  golds = gold_standards.nil? ? [] : JSON.parse(gold_standards)
  { :instructions => instructions,
    :question => JSON.dump(JSON.parse(question)),
    :type => type,
    :nonce => nonce || "",
    :gold_standards => JSON.dump(golds),
    :samples => (samples.nil? ? 1 : samples.to_i),
    :max_gold_standards => (max_gold_standards.nil? ? ([golds.length,3].min) : max_gold_standards),
    :gs_failure_rate => (gs_failure_rate.nil? ? 50 : gs_failure_rate)
  }
end

def put_ask!(params)
  data_type = DataType.find_by_dtype(params[:type])
  throw "bad data type" if data_type.nil?
  ARb.transaction {
    question_type = QuestionType.find_or_create_by_instructions_and_data_type_id_and_samples_and_gold_standards_and_max_gold_standards_and_gs_failure_rate(params[:instructions], data_type.id, params[:samples], params[:gold_standards], params[:max_gold_standards], params[:gs_failure_rate])
    question = Question.find_or_create_by_question_type_id_and_question(question_type.id, params[:question])
    ask = Ask.find_or_create_by_question_id_and_nonce(question.id, params[:nonce])
  }
  nil
end

def get_ask(params)
  data_type = DataType.find_by_dtype(params[:type])
  return nil if data_type.nil?
  question_type = QuestionType.find_by_instructions_and_data_type_id_and_samples_and_gold_standards_and_max_gold_standards_and_gs_failure_rate(params[:instructions], data_type.id, params[:samples], params[:gold_standards], params[:max_gold_standards], params[:gs_failure_rate])
  return nil if question_type.nil?
  question = Question.find_by_question_type_id_and_question(question_type.id, params[:question])
  return nil if question.nil?
  ask = Ask.find_by_question_id_and_nonce(question.id, params[:nonce])
  return nil if ask.nil?
  ask.answers.map(&:answer)
end
    
