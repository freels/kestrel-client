module Kestrel
  class Client
    class Unmarshal < Proxy
      def get(key, opts = {})
        response = client.get(key, opts.merge(:raw => true))
        return response if opts[:raw]

        if is_marshaled?(response)
          Marshal.load_with_constantize(response, loaded_constants = [])
        else
          response
        end
      end

      def is_marshaled?(object)
        object.to_s[0] == Marshal::MAJOR_VERSION && object.to_s[1] == Marshal::MINOR_VERSION
      rescue Exception
        false
      end
    end
  end
end
