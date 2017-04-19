
struct Sower <: AbstractSower
    snk::Any
    schema::Data.Schema

    function Sower(sink, schema::Data.Schema)
        new(sink, schema)
    end
    function Sower(sink)
        Sower(sink, Data.schema(sink))
    end
end

