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

    config_param :port,            :integer, :default => 1463
    config_param :bind,            :string,  :default => '0.0.0.0'
    config_param :server_type,     :string,  :default => 'nonblocking'
    config_param :is_framed,       :bool,    :default => true
    config_param :body_size_limit, :size,    :default => 32*1024*1024  # TODO default
    config_param :add_prefix,      :string,  :default => nil
    config_param :remove_newline,  :bool,    :default => false
    config_param :msg_format,      :string,  :default => 'text'

    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    def initialize
      require 'cgi'
      require 'thrift'
      $:.unshift File.join(File.dirname(__FILE__), 'thrift')
      require 'fb303_types'
      require 'fb303_constants'
      require 'facebook_service'
      require 'scribe_types'
      require 'scribe_constants'
      require 'scribe'
      super
    end

    def configure(conf)
      super
    end

    def start
      log.debug "listening scribe on #{@bind}:#{@port}"

      handler = FluentScribeHandler.new
      handler.add_prefix = @add_prefix
      handler.remove_newline = @remove_newline
      handler.msg_format = @msg_format
      handler.logger = log
      processor = Scribe::Processor.new handler

      @transport = Thrift::ServerSocket.new @bind, @port
      if @is_framed
        transport_factory = Thrift::FramedTransportFactory.new
      else
        transport_factory = Thrift::BufferedTransportFactory.new
      end

      unless ['text', 'json', 'url_param'].include? @msg_format
        raise 'Unknown format: msg_format=#{@msg_format}'
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
    rescue => e
      log.error "unexpected error", :error => e.inspect
      log.error_backtrace
    end

    class FluentScribeHandler
      attr_accessor :add_prefix
      attr_accessor :remove_newline
      attr_accessor :msg_format
      attr_accessor :logger # Use logger instead of log to avoid confusion with Log method

      def Log(msgs)
        begin
          msgs.each { |msg|
            record = create_record(msg)
            if @add_prefix
              Engine.emit(@add_prefix + '.' + msg.category, Engine.now, record)
            else
              Engine.emit(msg.category, Engine.now, record)
            end
          }
          return ResultCode::OK
        rescue => e
          logger.error "unexpected error", :error => e.inspect
          logger.error_backtrace
          return ResultCode::TRY_LATER
        end
      end

      private
      def create_record(msg)
        case @msg_format
        when 'text'
          if @remove_newline
            return { 'message' => msg.message.force_encoding('UTF-8').chomp }
          else
            return { 'message' => msg.message.force_encoding('UTF-8') }
          end
        when 'json'
          js = JSON.parse(msg.message.force_encoding('UTF-8'))
          raise 'body must be a Hash, if json_body=true' unless js.is_a?(Hash)
          return js
        when 'url_param'
          s = msg.message.force_encoding('UTF-8')
          return Hash[ s.split('&').map { |kv|
              k,v = kv.split('=', 2);
              [CGI.unescape(k), CGI.unescape(v)]
            }
          ]
        else
          raise 'Invalid format: #{@msg_format}'
        end
      end
    end
  end
end
