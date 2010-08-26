module Kestrel
  class Client
    class Blocking < Proxy
      SLEEP_TIME = 0.5

      def get(*args)
        times = 0

        loop do
          times += 1

          if response = client.get(*args)
            return response
          end

          sleep SLEEP_TIME if times > 5
        end
      end

      def get_without_blocking(*args)
        client.get(*args)
      end

    end
  end
end
