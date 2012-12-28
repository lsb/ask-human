require 'json'
require './ask-human'
require 'sqlite3'

DB = SQLite3::Database.new("turk.db")
DB.results_as_hash = true
DB.type_translation = true

def newest_batch
  maybe_ask_ids = DB.get_first_value("select ask_ids from old_and_full_batches limit 1")
  maybe_ask_ids.nil? ? [] : maybe_ask_ids.scan(/\d+/).map(&:to_i)
end

def ship!(ask_ids)
  # we assume that all the questions have the same question_type.
  prng = lambda {|s| Digest::MD5.hexdigest(ask_ids.sort.join + s) }
  prng_a = lambda {|a| prng[a.join] }
  asks = DB.execute("select asks.id, question, question_type_id from asks join questions on question_id = questions.id where asks.id in (#{['?'].*(ask_ids.size).join(',')})", *ask_ids)
  qt = DB.get_first_row("select * from question_types where id = ?", asks.first['question_type_id'])
  dtype = DB.get_first_value("select dtype from data_types where id = ?", qt['data_type_id'])
  golds = JSON.parse(qt["gold_standards"]).sort_by(&prng_a)[0, qt["max_gold_standards"]]
  inquiries = gold_inquiries = []
  if dtype == "radio"
    inquiries = asks.map {|a|
      q = JSON.parse(a["question"])
      answers = q[1..-1].each_with_index.map {|ans,i| {:id => i.to_s, :text => ans }}
      {:id => a["id"].to_s, :text => q[0], :answers => answers}
    }
    gold_inquiries = golds.map {|g|
      answers = g[1..-1].each_with_index.map {|ans,i| {:id => i.to_s, :text => ans }}.sort_by {|h| prng_a[ [g[0],h[:text]] ] }
      {:text => g.first, :answers => answers, :correct => '0'}
    }
  elsif dtype == "text"
    inquiries = asks.map {|a|
      q = JSON.parse(a["question"])
      {:id => a["id"].to_s, :text => q[0], :default => (q.size == 1 ? "" : q[1])}
    }
    gold_inquiries = golds.map {|g|
      {:text => g[0], :default => g[1], :correct => g[2]}
    }
  else
    throw "trying to ship unknown data type"
  end
  batch = inquiries + gold_inquiries
  throw "too many golds" if seconds_to_read(gold_inquiries, dtype.to_sym) > 3600
  return ship!(ask_ids.sort[0, ask_ids.size/2]) if seconds_to_read(batch, dtype.to_sym) > 3600
  hit_id = simple_ask_questions(batch, dtype.to_sym, qt["instructions"], qt["samples"])
  DB.transaction {
    DB.execute("insert into hits (id) values (?)", hit_id)
    asks.each {|a| DB.execute("insert into shipped_asks (ask_id, hit_id) values (?,?)", a["id"], hit_id) }
  }
  nil
end

def consume!()
  qt = lambda {|hit_id| DB.get_first_row("select qt.* from question_types qt join questions q on q.question_type_id = qt.id join asks a on a.question_id = q.id join shipped_asks sa on a.id = ask_id join hits h on h.id = hit_id where h.id = ?", hit_id) }
  max_gs_failure = lambda {|hit_id|
    maybe_qt = qt[hit_id]
    maybe_qt.nil? ? 110 : maybe_qt["gs_failure_rate"]/100.0
  }
  hit_id_to_samples = lambda {|hit_id|
    maybe_qt = qt[hit_id]
    maybe_qt.nil? ? 1 : maybe_qt["samples"]
  }
  consume_hit = lambda {|hit_id, assignment|
    maybe_qt = qt[hit_id]
    return STDERR.puts("Warning, disregarding HIT #{hit_id}") if maybe_qt.nil?
    dtype = DB.get_first_value("select dtype from data_types where id = ?", maybe_qt["data_type_id"])
    DB.transaction {
      mgsfr = max_gs_failure[hit_id]
      assignment.each {|a|
        next unless DB.get_first_value("select id from assignments where id = ?", a.fetch(:id)).nil?
        DB.execute("insert into assignments (id, hit_id, worker_id, assignment) values (?,?,?,?)", a.fetch(:id), hit_id, a.fetch(:worker_id), JSON.dump(a.fetch(:assignment)))
        if gs_failure_rate(a) < mgsfr
          a.fetch(:assignment).each {|ask_id, q_answer|
            next if ask_id.start_with?("g")
            question = DB.get_first_value("select question from questions join asks on question_id = questions.id where asks.id = ?", ask_id.to_i)
            answer = (dtype == "text" ? q_answer : (dtype == "radio" ? JSON.parse(question)[q_answer.to_i + 1] : throw("unsupported datatype")))
            DB.execute("insert into answers (assignment_id, ask_id, answer) values (?,?,?)", a.fetch(:id), ask_id.to_i, answer)
          }
        end
      }
    }
  }
  consume_hits(consume_hit, hit_id_to_samples, max_gs_failure) unless DB.get_first_value("select * from unanswered_hit_count").zero?
end

def fill_in_putget_ask(instructions, question, type, gold_standards, nonce, samples, max_gold_standards, gs_failure_rate)
  golds = gold_standards.nil? ? [] : JSON.parse(gold_standards)
  { :instructions => instructions,
    :question => JSON.dump(JSON.parse(question)),
    :type => DB.get_first_value("select dtype from data_types where dtype = ?", type) || throw("invalid data type"),
    :nonce => nonce || "",
    :gold_standards => JSON.dump(golds),
    :samples => (samples.nil? ? 1 : samples.to_i),
    :max_gold_standards => (max_gold_standards.nil? ? ([golds.length,3].min) : max_gold_standards),
    :gs_failure_rate => (gs_failure_rate.nil? ? 50 : gs_failure_rate)
  }
end

def put_ask!(params)
  data_type_id = DB.get_first_value("select id from data_types where dtype = ?", params[:type]) || throw("invalid data type")
  qt = {'instructions' => params[:instructions], 'data_type_id' => data_type_id, 'samples' => params[:samples], 'gold_standards' => params[:gold_standards], 'max_gold_standards' => params[:max_gold_standards], 'gs_failure_rate' => params[:gs_failure_rate]}
  DB.transaction {
    DB.execute("insert into question_types (instructions, data_type_id, samples, gold_standards, max_gold_standards, gs_failure_rate) values (:instructions, :data_type_id, :samples, :gold_standards, :max_gold_standards, :gs_failure_rate)", qt)
    question_type_id = DB.get_first_value("select id from question_types where instructions = :instructions and data_type_id = :data_type_id and samples = :samples and gold_standards = :gold_standards and max_gold_standards = :max_gold_standards and gs_failure_rate = :gs_failure_rate", qt)
    q = {"question_type_id" => question_type_id, "question" => params[:question]}
    DB.execute("insert into questions (question_type_id, question) values (:question_type_id, :question)", q)
    question_id = DB.get_first_value("select id from questions where question_type_id = :question_type_id and question = :question", q)
    a = {"question_id" => question_id, "nonce" => params[:nonce]}
    DB.execute("insert into asks (question_id, nonce) values (:question_id, :nonce)", a)
  }
  nil
end

def get_ask(params)
  data_type_id = DB.get_first_value("select id from data_types where dtype = ?", params[:type])
  return nil if data_type_id.nil?
  question_type_id = DB.get_first_value("select id from question_types where instructions = ? and data_type_id = ? and samples = ? and gold_standards = ? and max_gold_standards = ? and gs_failure_rate = ?", params[:instructions], data_type_id, params[:samples], params[:gold_standards], params[:max_gold_standards], params[:gs_failure_rate])
  return nil if question_type_id.nil?
  question_id = DB.get_first_value("select id from questions where question_type_id = ? and question = ?", question_type_id, params[:question])
  return nil if question_id.nil?
  ask_id = DB.get_first_value("select id from asks where question_id = ? and nonce = ?", question_id, params[:nonce])
  return nil if ask_id.nil?
  hit_id = DB.get_first_value("select h.id from hits h join shipped_asks sa join asks a on h.id = sa.hit_id and a.id = sa.ask_id where a.id = ?", ask_id)
  return nil if hit_id.nil?
  return nil if DB.get_first_value("select count(*) from assignments where hit_id = ?", hit_id).zero?
  DB.execute("select answer from answers where ask_id = ?", ask_id).map {|r| r["answer"] }
end
    
