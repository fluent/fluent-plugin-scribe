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

class ScribeOutput < BufferedOutput
  Fluent::Plugin.register_output('scribe', self)

  config_param :host,      :string,  :default => 'localhost'
  config_param :port,      :integer, :default => 1463
  config_param :field_ref, :string,  :default => 'message'
  config_param :timeout,   :integer, :default => 30

  config_param :remove_prefix,    :string, :default => nil
  config_param :default_category, :string, :default => 'unknown'

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

    if @remove_prefix
      @removed_prefix_string = @remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    if @remove_prefix and
        ( (tag[0, @removed_length] == @removed_prefix_string and tag.length > @removed_length) or
          tag == @remove_prefix)
      [(tag[@removed_length..-1] || @default_category), record].to_msgpack
    else
      [tag, record].to_msgpack
    end
  end

  def write(chunk)
    records = []
    chunk.msgpack_each { |arr|
      records << arr
    }

    socket = Thrift::Socket.new @host, @port, @timeout
    transport = Thrift::FramedTransport.new socket
    protocol = Thrift::BinaryProtocol.new transport, false, false
    client = Scribe::Client.new protocol

    transport.open
    begin
      entries = []
      records.each { |r|
        tag, record = r
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
