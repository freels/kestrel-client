class Kestrel::Client::Transactional < Kestrel::Client::Proxy

  # Raised when a caller attempts to use this proxy across
  # multiple queues.
  class MultipleQueueException < StandardError; end

  class RetryableJob < Struct.new(:retries, :job); end

  # Number of times to retry a job before giving up
  DEFAULT_RETRIES = 100

  # Pct. of the time during 'normal' processing we check the error queue first
  ERROR_PROCESSING_RATE = 0.1

  # ==== Parameters
  # client<Kestrel::Client>:: Client
  # max_retries<Integer>:: Number of times to retry a job before
  #                        giving up. Defaults to DEFAULT_RETRIES
  # error_rate<Float>:: Pct. of the time during 'normal'
  #                     processing we check the error queue
  #                     first. Defaults to ERROR_PROCESSING_RATE
  # per_server<Integer>:: Number of gets to execute against a
  #                       single server, before changing
  #                       servers. Defaults to MAX_PER_SERVER
  #
  def initialize(client, max_retries = nil, error_rate = nil)
    @max_retries = max_retries || DEFAULT_RETRIES
    @error_rate  = error_rate || ERROR_PROCESSING_RATE
    @counter     = 0 # Command counter
    super(client)
  end

  attr_reader :current_queue

  # Returns job from the +key+ queue 1 - ERROR_PROCESSING_RATE
  # pct. of the time. Every so often, checks the error queue for
  # jobs and returns a retryable job.
  #
  # ==== Returns
  # Job, possibly retryable, or nil
  #
  def get(key, opts = {})
    raise MultipleQueueException if current_queue && key != current_queue

    close_transaction(queue_for_last_job) if @job

    queue = (rand < @error_rate) ? key + "_errors" : key

    if job = client.get(queue, opts.merge(:open => true))
      @current_queue = key
      @job = job.is_a?(RetryableJob) ? job : RetryableJob.new(0, job)
      @job.job
    else
      @current_queue = @job = nil
    end
  end

  def queue_for_last_job
    if @job.retries < 1
      @current_queue
    else
      current_queue + "_errors"
    end
  end

  # Enqueues the current job on the error queue for later
  # retry. If the job has been retried DEFAULT_RETRIES times,
  # gives up entirely.
  #
  # ==== Returns
  # Boolean:: true if the job is retryable, false otherwise
  #
  def retry(item = nil)
    job =
      if item
        current_retries = (@job ? @job.retries : 0)
        RetryableJob.new(current_retries, item)
      else
        @job
      end

    return unless job

    job.retries += 1

    if job.retries == 1
      client.set(current_queue + "_errors", job)
      close_transaction(current_queue)
    elsif job.retries < @max_retries
      client.set(current_queue + "_errors", job)
      close_transaction(current_queue + "_errors")
    else
      close_transaction(current_queue + "_errors")
    end
    
    # No longer have an active job
    @current_queue = @job = nil
    job.retries < @max_retries
  end

  private

  def close_transaction(key) #:nodoc:
    client.get_from_last("#{key}/close")
  end
end
