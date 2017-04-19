

struct Resower <: AbstractSower
    snk::Any
    schema::Data.Schema

    h::Harvester
    migratecols::Vector{Symbol}

    function Resower(sink, schema::Data.Schema, h::Harvester, mcols::Vector{Symbol})
        new(sink, schema, h, mcols)
    end
    function Resower(sink, h::Harvester, mcols::Vector{Symbol})
        Resower(sink, Data.schema(sink), h, mcols)
    end
end


# it is expected that this idx and y are not for the complete dataset
function resow{T}(rs::Resower, idx::AbstractVector{<:Integer}, y::Matrix{T},
                  batch_size::Integer=DEFAULT_SOW_BATCH_SIZE)

end

