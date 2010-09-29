class Kestrel::Client::Transactional < Kestrel::Client::Proxy

  # Raised when a caller attempts to use this proxy across
  # multiple queues.
  class MultipleQueueException < StandardError; end


  class RetryableJob < Struct.new(:retries, :job); end


  # Number of times to retry a job before giving up
  DEFAULT_RETRIES = 100


  # Pct. of the time during 'normal' processing we check the error queue first
  ERROR_PROCESSING_RATE = 0.1


  # Maximum number of gets to execute before switching servers
  MAX_PER_SERVER = 100_000


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
  def initialize(client, max_retries = nil, error_rate = nil, per_server = nil)
    @max_retries = max_retries || DEFAULT_RETRIES
    @error_rate  = error_rate || ERROR_PROCESSING_RATE
    @per_server  = per_server || MAX_PER_SERVER
    @counter     = 0 # Command counter
    super(client)
  end

  attr_reader :current_queue

  # Returns job from the +key+ queue 1 - ERROR_PROCESSING_RATE
  # pct. of the time. Every so often, checks the error queue for
  # jobs and returns a retryable job. If either the error queue or
  # +key+ queue are empty, attempts to pull a job from the
  # alternate queue before giving up.
  #
  # ==== Returns
  # Job, possibly retryable, or nil
  #
  def get(key, opts = {})
    raise MultipleQueueException if current_queue && key != current_queue

    close_transaction(current_try == 1 ? key : "#{key}_errors")

    q1, q2 = (rand < @error_rate) ? [key + "_errors", key] : [key, key + "_errors"]

    if job = get_with_fallback(q1, q2, opts.merge(:close => true, :open => true))
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
  def retry
    return unless @job

    @job.retries += 1

    if should_retry = @job.retries < @max_retries
      client.set(current_queue + "_errors", @job)
    end

    # close the transaction on the original queue if this is the first retry
    close_transaction(@job.retries == 1 ? current_queue : "#{current_queue}_errors")

    should_retry
  end

  private

  # If a get against the +primary+ queue is nil, falls back to the
  # +secondary+ queue.
  #
  def get_with_fallback(primary, secondary, opts) #:nodoc:
    client.get(primary, opts) || client.get(secondary, opts)
  end

  def close_transaction(key) #:nodoc:
    client.get_from_last("#{key}/close")
  end
end
