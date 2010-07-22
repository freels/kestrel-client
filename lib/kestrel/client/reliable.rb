module Kestrel
  class Client
    class Reliable < Proxy

      def get(key, opts = false)
        opts = extract_options(opts)
        opts.merge! :close => true, :open => true
        client.get(key, opts)
      end

      def abort(key)
        client.get_from_last(key + "/abort")
      end
    end
  end
end
