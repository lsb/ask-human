require 'active_record'
require 'json'
require './ask-human'
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
  has_one :hit, :through => :shipped_ask
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
  attr_accessible :id, :worker_id, :assignment, :hit_id
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
  dtype = asks.first.question.question_type.data_type.dtype
  qt = asks.first.question.question_type
  golds = JSON.parse(qt.gold_standards).sort_by(&prng_a)[0, qt.max_gold_standards]
  inquiries = gold_inquiries = []
  if dtype == "radio"
    inquiries = asks.map {|a|
      q = JSON.parse(a.question.question)
      answers = q[1..-1].each_with_index.map {|ans,i| {:id => i.to_s, :text => ans }}
      {:id => a.id.to_s, :text => q[0], :answers => answers}
    }
    gold_inquiries = golds.map {|g|
      answers = g[1..-1].each_with_index.map {|ans,i| {:id => i.to_s, :text => ans }}.sort_by {|h| prng_a[ [g[0],h[:text]] ] }
      {:text => g.first, :answers => answers, :correct => '0'}
    }
  elsif dtype == "text"
    inquiries = asks.map {|a|
      q = JSON.parse(a.question.question)
      {:id => a.id.to_s, :text => q[0], :default => (q.size == 1 ? "" : q[1])}
    }
    gold_inquiries = golds.map {|g|
      {:text => g[0], :default => g[1], :correct => g[2]}
    }
  else
    throw "trying to ship unknown data type"
  end
  batch = inquiries + gold_inquiries
  return ship!(ask_ids.sort[0, ask_ids.size/2]) if seconds_to_read(batch, dtype.to_sym) > 3600
  hit_id = simple_ask_questions(batch, dtype.to_sym, qt.instructions, qt.samples)
  ARb.transaction {
    Hit.create!(:id => hit_id)
    asks.each {|a| ShippedAsk.create!(:ask_id => a.id, :hit_id => hit_id) }
  }
  nil
end

def consume!()
  max_gs_failure = lambda {|hit_id| Hit.find(hit_id).asks.first.question.question_type.gs_failure_rate/100.0 }
  hit_id_to_samples = lambda {|hit_id| Hit.find(hit_id).asks.first.question.question_type.samples }
  consume_hit = lambda {|hit_id, assignment|
    dtype = Hit.find(hit_id).asks.first.question.question_type.data_type.dtype
    ARb.transaction {
      mgsfr = max_gs_failure[hit_id]
      assignment.each {|a|
        next unless Assignment.find_by_id(a.fetch(:id)).nil?
        Assignment.create!(:id => a[:id], :hit_id => hit_id, :worker_id => a[:worker_id], :assignment => JSON.dump(a[:assignment]))
        if gs_failure_rate(a) < mgsfr
          a[:assignment].each {|ask_id, q_answer|
            next if ask_id.starts_with?("g")
            ask = Ask.find(ask_id.to_i)
            answer = (dtype == "text" ? q_answer : (dtype == "radio" ? JSON.parse(ask.question.question)[q_answer.to_i + 1] : throw("unsupported datatype")))
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
    :type => DataType.find_by_dtype(type).dtype,
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
  qt = {instructions: params[:instructions], data_type_id: data_type.id, samples: params[:samples], gold_standards: params[:gold_standards], max_gold_standards: params[:max_gold_standards], gs_failure_rate: params[:gs_failure_rate]}
  QuestionType.create!(qt)
  question_type = QuestionType.first(:conditions => qt)
  q = {question_type_id: question_type.id, question: params[:question]}
  Question.create!(q)
  question = Question.first(:conditions => q)
  a = {question_id: question.id, nonce: params[:nonce]}
  Ask.create!(a)
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
  return nil if ask.nil? || ask.hit.nil? || ask.hit.assignments.empty?
  ask.answers.map(&:answer)
end
    
