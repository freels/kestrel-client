module Kestrel
  class Client
    #--
    # TODO: Pull out the transaction management logic into
    #       Client. This class should only be responsible for the
    #       retry semantics.
    # TODO: Ensure that errors are pushed onto the error queue on the
    #       same server on which the error occurred.
    #++
    class Reliable < Proxy
      class RetryableJob < Struct.new(:retries, :job); end

      # Number of times to retry a job before giving up
      DEFAULT_RETRIES = 100

      # Pct. of the time during 'normal' processing we check the error queue first
      ERROR_PROCESSING_RATE = 0.1

      # Maximum number of gets to execute before switching servers
      MAX_PER_SERVER = 100_000

      # ==== Parameters
      # client<Kestrel::Client>:: Client
      # retry_count<Integer>:: Number of times to retry a job before
      #                        giving up. Defaults to DEFAULT_RETRIES
      # error_rate<Float>:: Pct. of the time during 'normal'
      #                     processing we check the error queue
      #                     first. Defaults to ERROR_PROCESSING_RATE
      # per_server<Integer>:: Number of gets to execute against a
      #                       single server, before changing
      #                       servers. Defaults to MAX_PER_SERVER
      #
      def initialize(client, retry_count = nil, error_rate = nil, per_server = nil)
        @retry_count = retry_count || DEFAULT_RETRIES
        @error_rate  = error_rate || ERROR_PROCESSING_RATE
        @per_server  = per_server || MAX_PER_SERVER
        @counter     = 0 # Command counter
        super(client)
      end

      # Returns job from the +key+ queue 1 - ERROR_PROCESSING_RATE
      # pct. of the time. Every so often, checks the error queue for
      # jobs and returns a retryable job. If either the error queue or
      # +key+ queue are empty, attempts to pull a job from the
      # alternate queue before giving up.
      #
      # ==== Returns
      # Job, possibly retryable, or nil
      #
      def get(key, opts = false)
        job =
          if rand < @error_rate
            get_with_fallback(key + "_errors", key, opts)
          else
            get_with_fallback(key, key + "_errors", opts)
          end

        if job
          @key = key
          @job = job.is_a?(RetryableJob) ? job : RetryableJob.new(0, job)
          @job.job
        else
          @key = @job = nil
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

        close_open_transaction!
        @job.retries += 1

        if @job.retries < @retry_count
          client.set(@key + "_errors", @job)
          true
        else
          false
        end
      end

      private

      # If a get against the +primary+ queue is nil, falls back to the
      # +secondary+ queue.
      #
      # Also, this executes a get on the first request, then a get_from_last
      # on each ensuing request for @per_server requests. This keeps the
      # client "attached" to a single server for a period of time.
      #
      def get_with_fallback(primary, secondary, opts) #:nodoc:
        opts = extract_options(opts)
        opts.merge! :close => true, :open => true

        if @counter == 0
          close_open_transaction! if @job
          @counter += 1
          command = :get
        elsif @counter < @per_server
          # Open transactions are implicitly closed, here.
          # FIXME: If the client switches queues, it is possible to
          #        leave an open txn on the old queue, in this branch.
          @counter += 1
          command = :get_from_last
        else
          close_open_transaction! if @job
          @counter = 0
          command = :get
        end

        client.send(command, primary, opts) || client.send(command, secondary, opts)
      end

      def close_open_transaction! #:nodoc:
        if @job.retries == 0
          client.get_from_last(@key, :close => true, :open => false)
        else
          client.get_from_last(@key + "_errors", :close => true, :open => false)
        end
      end
    end
  end
end
