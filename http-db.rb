require './db-asks'
require 'sinatra'

def sanitize_params(params)
  keys = %w[instructions question type gold_standards nonce samples max_gold_standards gs_failure_rate qualifications]
  values = keys.map {|k| params[k] }
  dtype = DB.get_first_value("select dtype from data_types where dtype = ?", params["type"])
  required_values = values[0,3] + [dtype]
  halt(412, "Error: required parameters are instructions, question, and type.") unless required_values.all?
  fill_in_putget_ask( *values ) #rescue halt(400, "Error: invalid JSON.")
end

put('/ask') {
  sanitized = sanitize_params(params)
  put_ask!(sanitized)
  ""
}

get_ask = lambda {
  sanitized = sanitize_params(params)
  answers = get_ask(sanitized)
  halt(404,"Error: no answers found for that ask") if answers.nil?
  JSON.dump(answers)
}

get('/ask', &get_ask)
post('/ask', &get_ask) # because sometimes gold standards are long

get('/') {
  content_type 'text/plain'
  all_data_types = DB.execute("select dtype from data_types").map {|r| r["dtype"] }.join(" or ")
  ["Hi.",
   "",
   "The endpoint for asking a question is /ask, with the parameters described below.",
   "PUT a specific ask of a question, and poll to GET a response.",
   "Use the same parameters each time.",
   "",
   "Parameters:",
   " instructions (for a batch of 1 or more questions) *",
   " type (data type of question; currently #{all_data_types}) *",
   " question (JSON-encoded array, specific to each data type) *",
   " nonce (random string for uniqueness, default '')",
   " gold_standards (JSON-encoded array of questions, specific to each data type, default [])",
   " samples (maximum number of different workers to ask the question, default 1)",
   " max_gold_standards (maximum number of gold standards in a batch, default the lesser of 3 and the number of questions in gold_standards)",
   " gs_failure_rate (the percentage of gold standard question that need to fail above which the answer is discarded, default 50)",
   " qualifications (skill sets / background; currently 'english' for reading and writing competence, or 'usphone' for making calls in the United States, default none)",
   "",
   "Question format for radio: [question, answer1, answer2...]",
   "Gold standard question format for radio: [question, correct_answer, answer2...]",
   "Question format for text: [question, default_text]",
   "Gold standard question format for text: [question, default_text, correct_answer]",
   "",
   "PUT response: empty body.",
   "GET response: JSON array of answers.",
   "",
   "Questions are grouped into batches based on the combination of type, instructions, samples, gold_standards, max_gold_standards, and gs_failure_rate.",
   "An ask is a question with a specific nonce; only one question per batch.",
   "",
   "Note that samples=5 guarantees not more than 5 answers, but it does not does not guarantee 5 answers.",
   "Only samples from batches with passing gold standards are returned.",
   "If one or more batches have failing gold standards, the batches are re-requested to still more different workers.",
   "However, it's only worthwhile to keep going in one of the two circumstances:",
   "1. There are more batches with passing gold standards than with failing gold standards; or",
   "2. There are no batches that have passing gold standards, and the total number of batches needed is not less than the number of failed batches.",
   ].join("\n")
}

## ^-- user facing
## v-- maintenance

post('/o') {
  batch = newest_batch()
  ship!(batch) unless batch.empty?
  ""
}

post('/i') {
  consume!()
  ""
}

post('/b') {
  block!()
  ""
}
