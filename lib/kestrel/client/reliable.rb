module Kestrel
  class Client
    class Reliable < Proxy
      class RetryableJob < Struct.new(:retries, :job); end

      # Number of times to retry a job before giving up
      DEFAULT_RETRIES = 100

      # Pct. of the time during 'normal' processing we check the error queue first
      ERROR_PROCESSING_RATE = 0.1

      # ==== Parameters
      # client<Kestrel::Client>:: Client
      # retry_count<Integer>:: Number of times to retry a job before
      #                        giving up. Defaults to DEFAULT_RETRIES
      # error_rate<Float>:: Pct. of the time during 'normal'
      #                     processing we check the error queue
      #                     first. Defaults to ERROR_PROCESSING_RATE
      #
      def initialize(client, retry_count = nil, error_rate = nil)
        @retry_count = retry_count || DEFAULT_RETRIES
        @error_rate  = error_rate || ERROR_PROCESSING_RATE
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
        opts = extract_options(opts)
        opts.merge! :close => true, :open => true

        close_open_transaction! if @job

        job =
          if rand < @error_rate
            client.get(key + "_errors", opts) || client.get(key, opts)
          else
            client.get(key, opts) || client.get(key + "_errors", opts)
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
