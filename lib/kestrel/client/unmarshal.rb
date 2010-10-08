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
      
      if RUBY_VERSION.respond_to?(:getbyte)
        def is_marshaled?(object)
          o = object.to_s
          o.getbyte(0) == Marshal::MAJOR_VERSION && o.getbyte(1) == Marshal::MINOR_VERSION
        rescue Exception
          false
        end
      else
        def is_marshaled?(object)
          o = object.to_s
          o[0] == Marshal::MAJOR_VERSION && o[1] == Marshal::MINOR_VERSION
        rescue Exception
          false
        end
      end
    end
  end
end
