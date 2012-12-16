require './db-asks'
require 'sinatra'

def sanitize_params(params)
  keys = %w[instructions question type gold_standards nonce samples max_gold_standards gs_failure_rate]
  values = keys.map {|k| params[k] }
  fill_in_putget_ask( *values )
end

put('/ask') {
  put_ask!(sanitize_params(params))
  ""
}

get('/ask') {
  answers = get_ask(sanitize_params(params))
  halt 404 if answers.nil?
  JSON.dump(answers)
}


## ^-- user facing
## v-- maintenance

post('/out') {
  batch = newest_batch()
  ship!(batch) unless batch.empty?
  ""
}

post('/in') {
  consume!()
  ""
}
