module Kestrel
  class Client
    class Retrying < Proxy

      # Number of times to retry after connection failures
      DEFAULT_RETRY_COUNT = 5

      # Exceptions which are connection failures we retry after
      RECOVERABLE_ERRORS = [
                            Memcached::ServerIsMarkedDead,
                            Memcached::ATimeoutOccurred,
                            Memcached::ConnectionBindFailure,
                            Memcached::ConnectionFailure,
                            Memcached::ConnectionSocketCreateFailure,
                            Memcached::Failure,
                            Memcached::MemoryAllocationFailure,
                            Memcached::ReadFailure,
                            Memcached::ServerError,
                            Memcached::SystemError,
                            Memcached::UnknownReadFailure,
                            Memcached::WriteFailure
                           ]

      def initialize(client)
        @retry_count = DEFAULT_RETRY_COUNT
        super(client)
      end

      %w(set get delete).each do |method|
        class_eval "def #{method}(*args); retry_call(#{method.inspect}, *args) end", __FILE__, __LINE__
      end

      private

      def retry_call(method, *args) #:nodoc:
        begin
          tries ||= 0
          client.send(method, *args)
        rescue *RECOVERABLE_ERRORS
          if tries < @retry_count
            tries += 1
            retry
          else
            raise
          end
        end
      end

    end
  end
end
