module Kestrel::Client::RetryHelper

  private

  def with_retries #:nodoc:
    yield
  rescue *Kestrel::Client::RECOVERABLE_ERRORS
    tries ||= @exception_retry_limit + 1
    tries -= 1
    tries > 0 ? retry : raise
  end
end
