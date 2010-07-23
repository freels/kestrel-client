module Kestrel
  class Client
    class Reliable < Proxy

      # Number of times to retry a job before giving up
      DEFAULT_RETRIES = 100

      # Pct. of the time during 'normal' processing we check the error queue first
      ERROR_PROCESSING_RATE = 0.1

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

        job =
          if rand < ERROR_PROCESSING_RATE
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
        @job.retries += 1
        if @job.retries < DEFAULT_RETRIES
          client.set(@key + "_errors", @job)
          true
        else
          false
        end
      end

      class RetryableJob < Struct.new(:retries, :job)
      end
    end
  end
end
