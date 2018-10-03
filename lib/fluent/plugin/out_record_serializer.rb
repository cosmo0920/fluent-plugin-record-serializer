require 'fluent/plugin/output'
require 'fluent/plugin/record_serializer'

module Fluent::Plugin
  class RecordSerializerOutput < Output
    Fluent::Plugin.register_output('record_serializer', self)

    helpers :event_emitter

    config_param :tag, :string
    config_param :format, :string, :default => 'json'
    config_param :field_name, :string, :default => 'payload'

    include Fluent::RecordSerializer

    def process(tag, es)
      es.each { |time, record|
        begin
          serialized_record = serialize_record(@format, record)
        rescue => e
          log.warn "serialize error: #{e.message}"
          next
        end

        router.emit(@tag, time, {
          'tag' => @tag,
          @field_name => serialized_record
        })
      }
    end
  end
end
