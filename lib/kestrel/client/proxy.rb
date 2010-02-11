module Kestrel
  class Client
    class Proxy
      attr_reader :client

      def initialize(client)
        @client = client
      end

      def method_missing(method, *args, &block)
        client.send(method, *args, &block)
      end
    end
  end
end
