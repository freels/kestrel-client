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

    close_transaction(current_try == 1 ? key : "#{key}_errors") if @current_queue

    queue = (rand < @error_rate) ? key + "_errors" : key

    if job = client.get(queue, opts.merge(:open => true))
      @current_queue = key
      @job = job.is_a?(RetryableJob) ? job : RetryableJob.new(0, job)
      @job.job
    else
      @current_queue = @job = nil
    end
  end

  def current_try
    @job ? @job.retries + 1 : 1
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
        current_retries = (@job ?  @job.retries : 0)
        RetryableJob.new(current_retries, item)
      else
        @job
      end

    return unless job

    job.retries += 1

    if should_retry = job.retries < @max_retries
      client.set(current_queue + "_errors", job)
    else
      @current_queue = nil
    end

    # close the transaction on the original queue
    close_transaction(job.retries == 1 ? current_queue : "#{current_queue}_errors")

    should_retry
  end

  private

  def close_transaction(key) #:nodoc:
    client.get_from_last("#{key}/close")
  end
end
