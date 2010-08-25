module Kestrel
  class Client
    class Blocking < Proxy
      def get(*args)
        loop do
          response = client.get(*args)
          return response if response
        end
      end

      def get_without_blocking(*args)
        client.get(*args)
      end

    end
  end
end
