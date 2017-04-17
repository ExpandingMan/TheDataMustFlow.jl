

# TODO for now everything is one row at a time, at some point must do blocks


struct Harvester <: AbstractHarvester
    src::Any
    schema::Data.Schema

    Xcols::Vector{Symbol}
    ycols::Vector{Symbol}
    filtercols::Vector{Symbol}

    batch_size::UInt32

    function Harvester(src, sch::Data.Schema, Xcols::AbstractVector{Symbol},
                       ycol::AbstractVector{Symbol}, filtercols::AbstractVector{Symbol},
                       batch_size::Integer)
        new(src, sch, convert(Vector{Symbol}, Xcols), convert(Vector{Symbol}, ycols),
            convert(Vector{Symbol}, filtercols), UInt32(batch_size))
    end
    function Harvester(src, Xcols::AbstractVector{Symbol}, ycols::AbstractVector{Symbol},
                       filtercols::AbstractVector{Symbol}, batch_size::Integer)
        Harvester(src, Data.schema(src), Xcols, ycols, filtercols, batch_size)
    end
end



# TODO: for now I'm writing a function to get the whole dataset, just to figure out how
# this is going to work
function harvest{T}(h::Harvester, f::StreamFilter, ::Type{T})

end



