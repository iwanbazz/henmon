# frozen_string_literal: true

require 'rufus-scheduler'
require 'json'

scheduler = Rufus::Scheduler.new

def check_hen_scraper(name)
  command = `hen scraper stats #{name}`
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

# Define the input argument
name = ARGV[0]
time = ARGV[1]
condition = ARGV[2]
puts "monitoring #{name} scraper starting every #{time}"
puts condition

scheduler.every time do
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
  puts "time_stamp: #{result['time_stamp']}"
  puts '----------------------------------------'

  if result['job_status'] == 'paused' && (result['fetching_failed']).positive?
    refetch = refetch(name)
    if refetch['status'] == 'to_process'
      resume_start = resume(name)
      puts "Refetch then resume: #{resume_start}"
    end
  elsif result['job_status'] == 'paused' && result['fetching_failed'].zero? && (result['refetch_failed']).positive?
    reparse_failed = reparse_on_failed(name)
    if reparse_failed['status'] == 'to_process'
      resume_start = resume(name)
      puts "Reparse then resume: #{resume_start}"
    end
  elsif result['job_status'] == 'paused' && result['fetching_failed'].zero?
    resume_start = resume(name)
    puts "Resume: #{resume_start}"
  elsif (result['fetching_failed']).positive?
    if condition.nil?
      refetch = refetch(name)
      puts "Refetch status: #{refetch}"
    end
  elsif (result['parsing_failed']).positive?
    unless condition.nil?
      refetch_parse = refetch_parse(name)
      puts "Refetch status: #{refetch_parse}"
    end
  elsif result['job_status'] == 'done'
    puts `Scraper done at: #{result['time_stamp']}`
    scheduler.shutdown
  end
end

# Keep the script running
scheduler.join
