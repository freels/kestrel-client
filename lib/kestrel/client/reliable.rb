module Kestrel
  class Client
    class Reliable < Proxy
      def get(key, *args, &block)
        client.get(key + "/close/open", *args)
      end

      def abort(key)
        client.get_from_last(key + "/abort")
      end
    end
  end
end
