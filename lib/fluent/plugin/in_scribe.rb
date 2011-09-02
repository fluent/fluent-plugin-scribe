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
    if tag = conf['tag']
      @tag = tag
    else
      raise ConfigError, "tail: 'tag' parameter is required on scribe input"
    end

    if body_size_limit = conf['body_size_limit']
      @body_size_limit = Config.size_value(body_size_limit)
    end
  end

  def start
    $log.debug "listening scribe on #{@bind}:#{@port}"

    handler = FluentScribeHandler.new @tag
    processor = Scribe::Processor.new handler

    @transport = Thrift::ServerSocket.new @host, @port
    transport_factory = Thrift::FramedTransportFactory.new

    # @server = Thrift::ThreadPoolServer.new processor, @transport, transport_factory
    @server = Thrift::NonblockingServer.new processor, @transport, transport_factory
    @server.serve
  end

  def shutdown
    @transport.close unless @transport.closed?
  end

  def run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  class FluentScribeHandler
    def initialize(tag)
      @tag = tag
    end

    def Log(msgs)
      begin
        msgs.each { |msg|
          event = Event.new(Engine.now, {
            'category' => msg.category,
            'message' => msg.message
          })
          Engine.emit(@tag, event)
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

