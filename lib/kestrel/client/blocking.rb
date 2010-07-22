module Kestrel
  class Client
    class Blocking < Proxy
      DEFAULT_TIMEOUT = 250

      def get(key, opts = false)
        opts = extract_options(opts)
        opts[:timeout] = DEFAULT_TIMEOUT

        loop do
          response = client.get(key, opts)
          return response if response
        end
      end

      def get_without_blocking(*args)
        client.get(*args)
      end

    end
  end
end
