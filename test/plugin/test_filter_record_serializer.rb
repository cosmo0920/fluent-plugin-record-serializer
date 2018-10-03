require 'helper'
require 'fluent/plugin/filter_record_serializer'
require 'yaml'

exit unless defined?(Fluent::Filter)

class RecordSerializerFilterTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TAG = 'test.serialize'
  FORMAT = 'json'
  FIELD_NAME = 'payload'

  CONFIG = %[
    type record_modifier
  ]

  YAML_CONFIG = %[
    type record_modifier
    format yaml
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::RecordSerializerFilter).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal FORMAT, d.instance.format
    assert_equal FIELD_NAME, d.instance.field_name
  end

  def test_json
    record = {'spam' => 1, 'ham' => 'egg',}
    d = create_driver

    d.run(default_tag: TAG) do
      d.feed(record)
    end

    assert_equal [{'tag' => TAG, FIELD_NAME => record.to_json}], d.filtered.map { |e| e.last }
  end

  def test_yaml
    record = {'spam' => 1, 'ham' => 'egg',}
    d = create_driver YAML_CONFIG

    d.run(default_tag: TAG) do
      d.feed(record)
    end

    assert_equal [{'tag' => TAG, FIELD_NAME => record.to_yaml}], d.filtered.map { |e| e.last }
  end
end
