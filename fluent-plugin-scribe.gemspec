# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name          = "fluent-plugin-scribe"
  gem.version       = "0.10.14"

  gem.authors       = ["Kazuki Ohta", "TAGOMORI Satoshi"]
  gem.email         = ["kazuki.ohta@gmail.com", "tagomoris@gmail.com"]
  gem.summary       = %q{Scribe Input/Output plugin for Fluentd event collector}
  gem.description   = %q{Fluentd input/output plugin to handle Facebook scribed thrift protocol}
  gem.homepage      = "https://github.com/fluent/fluent-plugin-scribe"
  gem.license       = "APLv2"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency "rake"
  gem.add_development_dependency "test-unit", '~> 3.0.2'
  gem.add_runtime_dependency "fluentd"
  gem.add_runtime_dependency "thrift", "~> 0.8.0"
end
