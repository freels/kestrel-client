module Kestrel
  class Client
    class Namespace < Proxy
      def initialize(namespace, client)
        @namespace = namespace
        @matcher = /\A#{Regexp.escape(@namespace)}:(.+)/
        super(client)
      end

      %w(set get delete flush stat).each do |method|
        class_eval "def #{method}(key, *args); client.#{method}(namespace(key), *args) end", __FILE__, __LINE__
      end

      def available_queues
        client.available_queues.map {|q| in_namespace(q) }.compact
      end

      def in_namespace(key)
        if match = @matcher.match(key)
          match[1]
        end
      end

      def namespace(key)
        "#{@namespace}:#{key}"
      end
    end
  end
end
