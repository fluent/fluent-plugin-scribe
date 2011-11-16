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

class ScribeOutput < ObjectBufferedOutput
  Fluent::Plugin.register_output('scribe', self)

  config_param :host,      :string,  :default => 'localhost'
  config_param :port,      :integer, :default => 1463
  config_param :field_ref, :string,  :default => 'message'
  config_param :timeout,   :integer, :default => 30

  def initialize
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
    super
  end

  def shutdown
    super
  end

  def write_objects(tag, es)
    socket = Thrift::Socket.new @host, @port, @timeout
    transport = Thrift::FramedTransport.new socket
    protocol = Thrift::BinaryProtocol.new transport, false, false
    client = Scribe::Client.new protocol

    transport.open
    begin
      entries = []
      es.each { |time,record|
        next unless record.has_key?(@field_ref)
        entry = LogEntry.new
        entry.category = tag
        entry.message = record[@field_ref].force_encoding('ASCII-8BIT')
        entries << entry
      }
      client.Log(entries)
    ensure
      transport.close
    end
  end
end

end
