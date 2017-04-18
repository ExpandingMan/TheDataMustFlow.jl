

# TODO for now everything is one row at a time, at some point must do blocks


struct Harvester <: AbstractHarvester
    src::Any
    schema::Data.Schema

    Xcols::Vector{Symbol}
    ycols::Vector{Symbol}

    function Harvester(src, sch::Data.Schema, Xcols::AbstractVector{Symbol},
                       ycol::AbstractVector{Symbol})
        new(src, sch, convert(Vector{Symbol}, Xcols), convert(Vector{Symbol}, ycols))
    end
    function Harvester(src, Xcols::AbstractVector{Symbol}, ycols::AbstractVector{Symbol})
        Harvester(src, Data.schema(src), Xcols, ycols)
    end
end


Xcolidx(h::Harvester) = colidx(h, h.Xcols)
ycolidx(h::Harvester) = colidx(h, h.ycols)


function harvest{TX,Ty}(h::Harvester, idx::AbstractVector{<:Integer}, ::Type{TX}, ::Type{Ty})
    Xcols = Xcolidx(h)
    ycols = ycolidx(h)
    allcols = collect(Set(Xcols) ∪ Set(ycols))
    # to be continued
end



