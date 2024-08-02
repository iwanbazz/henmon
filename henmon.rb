# frozen_string_literal: true

require 'rufus-scheduler'
require 'json'

scheduler = Rufus::Scheduler.new

def check_hen_scraper(name)
  command = `hen scraper stats #{name} --live`
  JSON.parse(command)
end

def new_run(name)
  command = `hen scraper start #{name}`
  JSON.parse(command)
end

def pause(name)
  command = `hen scraper job pause #{name}`
  JSON.parse(command)
end

def resume(name)
  command = `hen scraper job resume #{name}`
  JSON.parse(command)
end

def refetch(name)
  command = `hen scraper page refetch #{name} --fetch-fail`
  JSON.parse(command)
end

def refetch_parse(name)
  command = `hen scraper page refetch #{name} --parse-fail`
  JSON.parse(command)
end

def reparse(name)
  command = `hen scraper page reparse #{name} --parse-fail`
  JSON.parse(command)
end

def reparse_on_failed(name)
  command = `hen scraper page reparse #{name} --status refetch_failed`
  JSON.parse(command)
end

def limbo(name)
  command = `hen scraper page limbo #{name} --status refetch_failed`
  JSON.parse(command)
end

# Define the input argument
name = ARGV[0]
time = ARGV[1]
condition = ARGV[2]
action = ARGV[3]
job_status = 'active'

# validation
if name.nil?
  puts 'error: you need to define scraper_name'
  puts 'usage: henmon scraper_name time_to_watch refetch_query'
  puts 'example: henmon spydeals_nl 15m true'
  abort
elsif time.nil?
  puts 'error: you need to define time_to_watch'
  puts 'usage: henmon scraper_name time_to_watch refetch_query'
  puts 'example: henmon spydeals_nl 15m true'
  abort
elsif !['true', 'false'].include? condition
  puts 'error: you need to define refetch_query with true or false'
  puts 'usage: henmon scraper_name time_to_watch refetch_query'
  puts 'example: henmon spydeals_nl 15m true'
  abort
elsif condition == 'true'
  puts 'refetch failed fetch pages if scraper paused'
elsif condition == 'false'
  puts 'failed fetch pages will not refetch if scraper paused'
end

puts "monitoring #{name} scraper starting every #{time}"

index = {}
count = 0

scheduler.every time do |job|
  result = check_hen_scraper(name)
  puts(result['scraper_name'])
  puts "job_id: #{result['job_id']}"
  puts "job_status: #{result['job_status']}"
  puts "to_fetch: #{result['to_fetch']}"
  puts "pages: #{result['pages']}"
  puts "fetching_failed: #{result['fetching_failed']}"
  puts "parsing_failed: #{result['parsing_failed']}"
  puts "refetch_failed: #{result['refetch_failed']}"
  puts "limbo: #{result['limbo']}"
  puts "outputs: #{result['outputs']}"
  puts "count: #{count}"
  puts "time_stamp: #{result['time_stamp']}"
  puts '----------------------------------------'

  index[count+=1] = result['outputs']

  if result['job_status'] == 'paused' && (result['fetching_failed']).positive?
    refetch = refetch(name)
    if refetch['status'] == 'to_process'
      resume_start = resume(name)
      puts "Refetch then resume: #{resume_start}"
      puts '----------------------------------------'
    end
  elsif result['job_status'] == 'paused' && result['fetching_failed'].zero? && (result['refetch_failed']).positive?
    reparse_failed = limbo(name)
    if reparse_failed['status'] == 'to_process'
      resume_start = resume(name)
      puts "Reparse then resume: #{resume_start}"
      puts '----------------------------------------'
    end
  elsif result['job_status'] == 'paused' && result['fetching_failed'].zero? && result['parsing_failed'].positive?
    refetch_parse = refetch_parse(name)
    if refetch_parse['status'] == 'to_process'
      resume_start = resume(name)
      puts "Refetch failed_parse then resume: #{resume_start}"
      puts '----------------------------------------'
    end
  elsif result['job_status'] == 'paused' && result['fetching_failed'].zero?
    resume_start = resume(name)
    puts "Resume: #{resume_start}"
    puts '----------------------------------------'
  elsif (result['fetching_failed']).positive?
    if condition.nil?
      refetch = refetch(name)
      puts "Refetch status: #{refetch}"
      puts '----------------------------------------'
    end
  # elsif (result['parsing_failed']).positive?
  #   unless condition.nil?
  #     refetch_parse = refetch_parse(name)
  #     puts "Refetch status: #{refetch_parse}"
  #     puts '----------------------------------------'
  #   end
  elsif result['job_status'] == 'done'
    if action.nil?
      job_status = result['job_status']
      puts `Scraper done at: #{result['time_stamp']}`
      puts '----------------------------------------'
      job.unschedule
      abort
      exit
      finish
    else
      start = new_run(name)
      puts "New run status: #{start}"
      puts '----------------------------------------'
    end
  end

  if index[count] != 0 && index[count-5] == index[count]
    pause_start = pause(name)
    puts "Pause to avoid stuck: #{pause_start}"
    puts '----------------------------------------'
    count = 0
  end
end

if job_status == 'active'
  # Keep the script running
  scheduler.join
else
  if action.nil?
    scheduler.unjoin
    abort
    exit
  end
end
