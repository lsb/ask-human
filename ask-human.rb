require 'rest-client'; require 'openssl'; require 'date'; require 'base64'; require 'rexml/document'; require 'builder'
require './turk-credentials'

def add_signature(params)
  timestamp = DateTime.now.xmlschema
  service = "AWSMechanicalTurkRequester"
  access_key_id = Turk.fetch(:access)
  operation = params.fetch("Operation")
  signature = Base64.encode64(OpenSSL::HMAC.digest("sha1", Turk.fetch(:secret_access), service+operation+timestamp)).strip
  params.merge({"Timestamp" => timestamp, "Service" => service, "AWSAccessKeyId" => access_key_id, "Signature" => signature})
end

def turk(params)
  RestClient.post(Turk.fetch(:endpoint), add_signature(params))
end

def generate_radiotext_question_form(radio_text, title, questions)
  Builder::XmlMarkup.new.QuestionForm(:xmlns => "http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2005-10-01/QuestionForm.xsd") {|b|
    b.Overview {
      b.Title(title)
    }
    questions.each {|q|
      b.Question {
        b.QuestionIdentifier(q[:id])
        b.IsRequired("true")
        b.QuestionContent {
          b.Text(q[:text].tr("\u2706","")) # TODO discover the limits of the character set
        }
        b.AnswerSpecification {
          if radio_text == :radio
            b.SelectionAnswer {
              b.StyleSuggestion("radiobutton")
              b.Selections {
                q[:answers].each {|a|
                  b.Selection {
                    b.SelectionIdentifier(a[:id])
                    b.Text(a[:text])
                  }
                }
              }
            }
          elsif radio_text == :text
            b.FreeTextAnswer {
              b.DefaultText(q[:default])
            }
          else
            throw "invalid question type"
          end
        }
      }
    }
  }
end

def generate_answer_key(answers)
  Builder::XmlMarkup.new.AnswerKey(:xmlns => "http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2005-10-01/AnswerKey.xsd") {|b|
    answers.each {|a|
      b.Question {
        b.QuestionIdentifier(a.fetch(:id))
        b.AnswerOption {
          b.SelectionIdentifier(a.fetch(:text))
          b.AnswerScore(a.fetch(:score, 1))
        }
      }
    }
  }
end

def create_qualification_type(name, description, retry_delay, question_form, answer_key, duration)
  qt = turk({ "Operation" => "CreateQualificationType",
              "Name" => name,
              "Description" => description,
              "QualificationTypeStatus" => "Active",
              "RetryDelayInSeconds" => retry_delay,
              "TestDurationInSeconds" => duration,
              "Test" => question_form,
              "AnswerKey" => answer_key })
  throw qt unless REXML::Document.new(qt).root.get_elements("//IsValid").map(&:text).first == "True"
  REXML::Document.new(qt).elements["//QualificationTypeId/text()"].to_s
end

def account_balance()
  ab = turk({"Operation" => "GetAccountBalance"})
  amount = REXML::Document.new(ab).elements['/GetAccountBalanceResponse/GetAccountBalanceResult/AvailableBalance/Amount/text()']
  throw ab if amount.nil?
  amount.to_s.to_f
end

def block_worker(worker_id, reason="Too many failed gold standards")
  bw = turk({"Operation" => "BlockWorker", "WorkerId" => worker_id, "Reason" => reason})
  throw bw unless REXML::Document.new(bw).root.get_elements("//IsValid").map(&:text).first == "True"
end

def bonus_worker(worker_id, assignment_id, amt, reason="Thanks for your good work.")
  bw = turk({"Operation" => "GrantBonus", "WorkerId" => worker_id, "AssignmentId" => assignment_id, "BonusAmount.1.Amount" => amt.round(2), "BonusAmount.1.CurrencyCode" => "USD", "Reason" => reason})
  throw bw unless REXML::Document.new(bw).root.get_elements("//IsValid").map(&:text).first == "True"
end

def create_hit(params)
  title = params.fetch(:title, "General Survey")
  description = params.fetch(:description)
  question = params.fetch(:question)
  reward = params.fetch(:reward)
  duration = params.fetch(:duration)
  lifetime = params.fetch(:lifetime, 3600 * 6)
  keywords = params.fetch(:keywords, []).join(',')
  max_assignments = params.fetch(:samples)
  auto_approve = 60
  requirements = params.fetch(:qualifications, []).map {|r|
    {"QualificationTypeId" => r.fetch('id'), "Comparator" => r.fetch('comparator'), r.fetch('value_type') => r.fetch('value'), "RequiredToPreview" => r.fetch('hidden', false)}
  }
  requirement_parameters = (0...requirements.size).map {|i| requirements[i].map {|k,v| [["QualificationRequirement",i+1,k].join('.'), v] } }.inject([], &:+).inject({}) {|h,kv| k,v = *kv; h[k] = v; h }

  unique_request_token = OpenSSL::Digest.hexdigest("md5", params.map(&:join).sort.join)

  h = turk({ "Operation" => "CreateHIT",
             "Title" => title,
             "Description" => description,
             "Question" => question,
             "Reward.1.CurrencyCode" => "USD",
             "Reward.1.Amount" => reward,
             "AssignmentDurationInSeconds" => duration,
             "LifetimeInSeconds" => lifetime,
             "Keywords" => keywords,
             "MaxAssignments" => max_assignments,
             "AutoApprovalDelayInSeconds" => auto_approve,
             "UniqueRequestToken" => unique_request_token,
           }.merge(requirement_parameters))

  hit_id = REXML::Document.new(h).elements['/CreateHITResponse/HIT/HITId/text()']
  throw "#{h.inspect}\n#{params.inspect}" if hit_id.nil?
  hit_id.to_s
end

def reviewable_hit_ids()
  rh = turk({"Operation" => "GetReviewableHITs"})
  REXML::Document.new(rh).root.get_elements("//HITId").map(&:text)
end

def assignments_per_hit(hit_id)
  # you can ask a question an infinite # of times, just getting a max of a hundred different workers at a time
  REXML::Document.new(turk({"Operation" => "GetAssignmentsForHIT", "HITId" => hit_id, "PageSize" => 100})).root.get_elements("//Assignment").map {|a|
    assignment_id = a.get_elements("AssignmentId").first.text
    worker_id = a.get_elements("WorkerId").first.text
    answer_xml = REXML::Document.new(a.get_elements("Answer").first.text).root
    assignment_values_hash = Hash[answer_xml.get_elements("Answer").map {|a|
                                    # TODO: pay down code debt, pass in (:: hit_id -> type)
                                    ["QuestionIdentifier", "FreeText", "SelectionIdentifier"].map {|i| a.get_elements(i).first}.compact.map(&:text)
                                  }]
    {:id => assignment_id, :worker_id => worker_id, :assignment => assignment_values_hash}
  }
end

def extend_hit(hit_id, increment=1)
  REXML::Document.new(turk({"Operation" => "ExtendHIT", "HITId" => hit_id, "MaxAssignmentsIncrement" => increment})).root.get_elements("//IsValid")[0].text == "True"
end

def dispose_hit(hit_id)
  REXML::Document.new(turk({"Operation" => "DisposeHIT", "HITId" => hit_id})).root.get_elements("//IsValid")[0].text == "True"
end

def mark_gs_question(q)
  q_text_h = Digest::MD5.hexdigest(q.fetch(:text))
  q_correct_h = Digest::MD5.hexdigest(q.fetch(:correct))
  q.merge({:id => "g#{q_text_h}#{q_correct_h}"})
end

def check_gs_question(qid,a)
  qid.end_with?(Digest::MD5.hexdigest(a))
end

def gs_failure_rate(a)
  golds = a.fetch(:assignment).find_all {|qid, a| qid.start_with?("g") }
  return 0 if golds.empty?
  gold_count = golds.size.to_f
  failed_gold_count = golds.find_all {|qid, a| !check_gs_question(qid,a) }.size.to_f
  failed_gold_count / gold_count
end

def seconds_to_read(questions, radio_text)
  words_per_second = 3.75 # = 225 wpm (brisk)
  joined_words = questions.map {|q| [q.fetch(:text), (radio_text == :text ? "" : q.fetch(:answers).map {|a| a.fetch(:text)} ) ].join(' ') }.join(' ')
  word_count = joined_words.scan(/\w+/).size.to_f
  multiplier = (radio_text == :radio ? 1 : 5)
  phone_count = joined_words.count("\u2706") # each unicode phone symbol signifies adding one minute
  (word_count / words_per_second + phone_count * 60) * multiplier
end

def time_allotment(questions, radio_text)
  min_seconds = 60 # as per amzn
  seconds = seconds_to_read(questions, radio_text).to_i * 5 # to be generous
  [seconds, min_seconds].max
end

def hit_reward(questions, radio_text)
  min_reward = 0.01
  reading_seconds = seconds_to_read(questions, radio_text)
  seconds_per_hour = 3600.0
  mturk_hourly_living_wage = 4.00 # high pay = quick results
  reward = ((reading_seconds / seconds_per_hour) * mturk_hourly_living_wage).round(2)
  [reward, min_reward].max
end

def simple_ask_questions(questions, radio_text, title, samples, qualifications=[], shuffle=true)
  marked_qs = questions.map {|q| q.has_key?(:correct) ? mark_gs_question(q) : q }
  qids = marked_qs.map {|q| q.fetch(:id) }.sort.join
  shuffled_marked_qs = marked_qs.sort_by {|q| Digest::MD5.hexdigest(qids + q[:id])}
  ordered_questions = shuffle ? shuffled_marked_qs : marked_qs
  reward = hit_reward(questions, radio_text)
  duration = time_allotment(questions, radio_text)
  question_form = generate_radiotext_question_form(radio_text, title, ordered_questions)
  create_hit({ :title => title, :description => title,
               :question => question_form,
               :reward => reward,
               :duration => duration,
               :qualifications => qualifications,
               :samples => samples })
end

def maybe_finished_hit(hit_id, max_gs_failure_rate, hit_id_to_samples)
  assignments = assignments_per_hit(hit_id)
  samples = hit_id_to_samples[hit_id]
  current_max_gs_failure_rate = max_gs_failure_rate[hit_id]
  successful_assignments = assignments.find_all {|a| gs_failure_rate(a) < current_max_gs_failure_rate }
  successful_size = successful_assignments.size
  samples_needed = samples - successful_size
  unsuccessful_size = assignments.size - successful_size
  worthwhile_to_keep_going = (successful_size >= unsuccessful_size) || (successful_size.zero? && samples_needed >= unsuccessful_size)
  #puts ">>> #{hit_id}: samples_needed=#{samples_needed}, successful_size=#{successful_size}, unsuccessful_size=#{unsuccessful_size}, worthwhile_to_keep_going=#{worthwhile_to_keep_going}"
  if samples_needed > 0 && worthwhile_to_keep_going
    extend_hit(hit_id, samples_needed)
    nil
  else
    [hit_id, assignments]
  end
end

def finished_hits(max_gs_failure_rate, hit_id_to_samples)
  Hash[reviewable_hit_ids().map {|hit_id|
    maybe_finished_hit(hit_id, max_gs_failure_rate, hit_id_to_samples)
  }.compact]
end

def consume_hits(consume_hit, hit_id_to_samples, max_gs_failure_rate)
  finished_hits(max_gs_failure_rate, hit_id_to_samples).each {|hit_id, assignments|
    consume_hit[hit_id, assignments]
    dispose_hit(hit_id)
  }
  nil
end
