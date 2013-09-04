#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'rake'
require 'rake/clean'

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

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task :default => :test
