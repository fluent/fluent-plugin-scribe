#
# Fluent
#
# Copyright (C) 2011 Kazuki Ohta
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
module Fluent


class ScribeInput < Input
  Plugin.register_input('scribe', self)

  def initialize
    require 'thrift'
    $:.unshift File.join(File.dirname(__FILE__), 'thrift')
    require 'fb303_types'
    require 'fb303_constants'
    require 'facebook_service'
    require 'scribe_types'
    require 'scribe_constants'
    require 'scribe'

    @port = 1463
    @bind = '0.0.0.0'
    @body_size_limit = 32*1024*1024  # TODO default
  end

  def configure(conf)
    @port = conf['port'] || @port
    @port = @port.to_i
    @bind = conf['bind'] || @bind

    @server_type = conf['server_type'] || 'thread_pool'
    @is_framed = conf['framed'].to_s != "false"

    if body_size_limit = conf['body_size_limit']
      @body_size_limit = Config.size_value(body_size_limit)
    end
  end

  def start
    $log.debug "listening scribe on #{@bind}:#{@port}"

    handler = FluentScribeHandler.new
    processor = Scribe::Processor.new handler

    @transport = Thrift::ServerSocket.new @host, @port
    if @is_framed
      transport_factory = Thrift::FramedTransportFactory.new
    else
      transport_factory = Thrift::BufferedTransportFactory.new
    end

    # 2011/09/29 Kazuki Ohta <kazuki.ohta@gmail.com>
    # This section is a workaround to set strict_read and strict_write option.
    # Ruby-Thrift 0.7 set them both 'true' in default, but Scribe protocol set
    # them both 'false'.
    protocol_factory = Thrift::BinaryProtocolFactory.new
    protocol_factory.instance_eval {|obj|
      def get_protocol(trans) # override
        return Thrift::BinaryProtocol.new(trans,
                                          strict_read=false,
                                          strict_write=false)
      end
    }

    case @server_type
    when 'simple'
      @server = Thrift::SimpleServer.new processor, @transport, transport_factory, protocol_factory
    when 'threaded'
      @server = Thrift::ThreadedServer.new processor, @transport, transport_factory, protocol_factory
    when 'thread_pool'
      @server = Thrift::ThreadPoolServer.new processor, @transport, transport_factory, protocol_factory
    when 'nonblocking'
      @server = Thrift::NonblockingServer.new processor, @transport, transport_factory, protocol_factory
    else
      raise ConfigError, "in_scribe: unsupported server_type '#{@server_type}'"
    end
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @transport.close unless @transport.closed?
    #@thread.join # TODO
  end

  def run
    @server.serve
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  class FluentScribeHandler
    def Log(msgs)
      begin
        msgs.each { |msg|
          event = Event.new(Engine.now, {
            'message' => msg.message
          })
          Engine.emit(msg.category, event)
        }
        return ResultCode::OK
      rescue => e
        $log.error "unexpected error", :error=>$!.to_s
        $log.error_backtrace
        return ResultCode::TRY_LATER
      end
    end
  end
end


end

