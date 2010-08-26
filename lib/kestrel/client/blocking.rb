module Kestrel
  class Client
    class Blocking < Proxy

      # random backoff sleeping

      SLEEP_TIMES = [[0] * 1, [0.01] * 2, [0.1] * 2, [0.5] * 2, [1.0] * 1].flatten

      def get(*args)
        count = 0

        while count += 1

          if response = client.get(*args)
            return response
          end

          sleep_for_count(count)
        end
      end

      def get_without_blocking(*args)
        client.get(*args)
      end

      private

      def sleep_for_count(count)
        base = SLEEP_TIMES[count] || SLEEP_TIMES.last

        time = ((rand * base) + base) / 2
        sleep time if time > 0
      end
    end
  end
end
