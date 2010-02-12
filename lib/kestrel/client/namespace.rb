module Kestrel
  class Client
    class Namespace < Proxy
      def initialize(namespace, client)
        @namespace = namespace
        super(client)
      end

      def get(key, *args)
        client.get(namespace(key), *args)
      end

      def set(key, *args)
        client.set(namespace(key), *args)
      end

      private

      def namespace(key)
        "#{@namespace}:#{key}"
      end
    end
  end
end