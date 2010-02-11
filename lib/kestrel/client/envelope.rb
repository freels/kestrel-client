module Kestrel
  class Client
    class Envelope < Proxy
      attr_accessor :envelope_class

      def initialize(envelope_class, client)
        @envelope_class = envelope_class
        super(client)
      end

      def get(*args)
        response = client.get(*args)
        if response.respond_to?(:unwrap)
          response.unwrap
        else
          response
        end
      end

      def set(key, value, *args)
        client.set(key, envelope_class.new(value), *args)
      end
    end
  end
end
