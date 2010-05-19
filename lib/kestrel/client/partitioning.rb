module Kestrel
  class Client
    class Partitioning < Proxy

      def initialize(client_map)
        @clients = client_map.inject({}) do |clients, (keys, client)|
          Array(keys).inject(clients) do |_, key|
            clients.update(key => client)
          end
        end
      end

      def clients
        @clients.values.uniq
      end

      def default_client
        @clients[:default]
      end
      alias client default_client

      %w(set get delete flush stat).each do |method|
        class_eval "def #{method}(key, *args); client_for(key).#{method}(key, *args) end", __FILE__, __LINE__
      end

      def stats
        merge_stats(clients.map {|c| c.stats })
      end

      def client_for(key)
        @clients[key.to_s] || default_client
      end
    end
  end
end
