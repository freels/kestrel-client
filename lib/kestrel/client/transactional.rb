class Kestrel::Client::Transactional < Kestrel::Client::Proxy

  # Raised when a caller attempts to use this proxy across
  # multiple queues.
  class MultipleQueueException < StandardError; end

  # Raised when a caller attempts to retry a job if
  # there is no current open transaction
  class NoOpenTransaction < StandardError; end

  class RetryableJob < Struct.new(:retries, :job); end

  # Number of times to retry a job before giving up
  DEFAULT_RETRIES = 10

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
  # ==== Raises
  # MultipleQueueException
  #
  def get(key, opts = {})
    raise MultipleQueueException if current_queue && key != current_queue

    close_last_transaction

    queue = read_from_error_queue? ? key + "_errors" : key

    if job = client.get(queue, opts.merge(:open => true))
      @job = job.is_a?(RetryableJob) ? job : RetryableJob.new(0, job)
      @last_read_queue = queue
      @current_queue = key
      @job.job
    end
  end

  def current_try
    @job.retries + 1
  end

  def close_last_transaction #:nodoc:
    return unless @last_read_queue

    client.get_from_last(@last_read_queue + "/close")
    @last_read_queue = @current_queue = @job = nil
  end

  # Enqueues the current job on the error queue for later
  # retry. If the job has been retried DEFAULT_RETRIES times,
  # gives up entirely.
  #
  # ==== Parameters
  # item (optional):: if specified, the job set to the error
  #                   queue with the given payload instead of what
  #                   was originally fetched.
  #
  # ==== Returns
  # Boolean:: true if the job is enqueued in the retry queue, false otherwise
  #
  # ==== Raises
  # NoOpenTransaction
  #
  def retry(item = nil)
    raise NoOpenTransaction unless @last_read_queue

    job = item ? RetryableJob.new(@job.retries, item) : @job.dup

    job.retries += 1

    client.set(current_queue + "_errors", job) if job.retries < @max_retries
    close_last_transaction

    job.retries < @max_retries
  end

  private

  def read_from_error_queue?
    rand < @error_rate
  end
end
