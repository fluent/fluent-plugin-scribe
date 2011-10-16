require 'rake'
require 'rake/testtask'
require 'rake/clean'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "fluent-plugin-scribe"
    gemspec.summary = "Scribe plugin for Fluent event collector"
    gemspec.author = "Kazuki Ohta"
    gemspec.email = "kazuki.ohta@gmail.com"
    gemspec.homepage = "https://github.com/fluent/fluent-plugin-scribe"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "fluentd", "~> 0.10.0"
    gemspec.add_dependency "thrift", "~> 0.7.0"
    gemspec.test_files = Dir["test/**/*.rb"]
    gemspec.files = Dir["bin/**/*", "lib/**/*", "test/**/*.rb"] +
      %w[example.conf VERSION AUTHORS Rakefile fluent-plugin-scribe.gemspec]
    gemspec.executables = ['fluent-scribe-remote']
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

task "thrift_gen" do
  system "rm -f common.thrift jobtracker.thrift"
  system "wget https://raw.github.com/facebook/scribe/master/if/scribe.thrift"
  system "wget https://raw.github.com/apache/thrift/trunk/contrib/fb303/if/fb303.thrift"
  system "mv scribe.thrift lib/fluent/plugin/thrift/"
  system "mv fb303.thrift lib/fluent/plugin/thrift/"
  system "mkdir -p tmp"
  system "sed -i '' 's/fb303\\/if\\///g' lib/fluent/plugin/thrift/scribe.thrift"
  system "thrift --gen rb -o tmp lib/fluent/plugin/thrift/fb303.thrift"
  system "thrift --gen rb -o tmp lib/fluent/plugin/thrift/scribe.thrift"
  system "mv tmp/gen-rb/* lib/fluent/plugin/thrift/"
  system "rm -fR tmp"
end

Rake::TestTask.new(:test) do |t|
  t.test_files = Dir['test/plugin/*.rb']
  t.ruby_opts = ['-rubygems'] if defined? Gem
  t.ruby_opts << '-I.'
end

#VERSION_FILE = "lib/fluent/version.rb"
#
#file VERSION_FILE => ["VERSION"] do |t|
#  version = File.read("VERSION").strip
#  File.open(VERSION_FILE, "w") {|f|
#    f.write <<EOF
#module Fluent
#
#VERSION = '#{version}'
#
#end
#EOF
#  }
#end
#
#task :default => [VERSION_FILE, :build]

task :default => [:build]
