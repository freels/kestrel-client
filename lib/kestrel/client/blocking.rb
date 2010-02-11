module Kestrel
  class Client
    class Blocking < Proxy
      DEFAULT_EXPIRY = 0
      WAIT_TIME_BEFORE_RETRY = 0.25

      def get(*args)
        while !(response = client.get(*args))
          sleep WAIT_TIME_BEFORE_RETRY
        end
        response
      end

      def get_without_blocking(*args)
        client.get(*args)
      end

      def set(key, value, expiry = DEFAULT_EXPIRY, raw = false)
        @retried = false
        begin
          client.set(key, value, expiry, raw)
        rescue Memcached::Error => e
          raise if @retried
          sleep(WAIT_TIME_BEFORE_RETRY)
          @retried = true
          retry
        end
      end
    end
  end
end
