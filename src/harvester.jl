

# TODO for now everything is one row at a time, at some point must do blocks


"""
# Type: `Harvester`

This data type wraps an object `src` that implements the `DataStreams` source interface.
It is used for reformatting data into raw matrices appropriate for machine learning.

See `harvest`.

## Constructors

    Harvester(src, Xcols[, ycols])

One should pass a source object from which to gather data which will be placed into matrices
`X` and `y`, the columns of which correspond to the columns of `src` specified by `Xcols`
and `ycols` respectively.
"""
struct Harvester <: AbstractHarvester
    src::Any
    schema::Data.Schema

    # TODO consider making this handle arbitrarily many matrices
    Xcols::Vector{Symbol}
    ycols::Vector{Symbol}

    transforms::Dict

    function Harvester(src, sch::Data.Schema, Xcols::AbstractVector{Symbol},
                       ycols::AbstractVector{Symbol})
        new(src, sch, Xcols, ycols)
    end
    function Harvester(src, Xcols::AbstractVector{Symbol}, ycols::AbstractVector{Symbol})
        Harvester(src, Data.schema(src), Xcols, ycols)
    end
    function Harvester(src, Xcols::AbstractVector{Symbol})
        Harvester(src, Xcols, Symbol[])
    end
end
export Harvester


Xcolidx(h::Harvester) = colidx(h.schema, h.Xcols)
ycolidx(h::Harvester) = colidx(h.schema, h.ycols)


# TODO for now we only support X and y of the same type
# TODO for now we only handle non-null vectors
function _harvest_batch{T}(h::Harvester, ::Type{T},
                           Xcolmap::Dict, Xwidth::Integer,
                           ycolmap::Dict, ywidth::Integer,
                           allcols::AbstractVector{<:Integer},
                           alltypes::AbstractVector{DataType},
                           bidx::AbstractVector{<:Integer})
    X = Matrix{T}(length(bidx), Xwidth)
    y = Matrix{T}(length(bidx), ywidth)
    for (dtype, c) ∈ zip(alltypes, allcols)
        v = coerce(h.src, T, dtype, bidx, c)
        Xcol = Xcolmap[c];  ycol = ycolmap[c]
        Xcol ≠ 0 && (X[:, Xcol] .= v)
        ycol ≠ 0 && (y[:, ycol] .= v)
    end
    X, y
end

"""
    harvester(h::Harvester, ::Type{T})

Return a function `h(idx)` which takes an `AbstractVector{<:Integer}` and returns the
`X,y` pairs as determined by the `Harvester`.  See the `Harvester` constructor for more
details.
"""
function harvester{T}(h::Harvester, ::Type{T})
    Xcols = Xcolidx(h);  Xwidth = length(Xcols)
    ycols = ycolidx(h);  ywidth = length(ycols)
    allcols = collect(Set(Xcols) ∪ Set(ycols))  # no reason to sort?
    _types = Data.types(h.schema)
    alltypes = DataType[_types[c] for c ∈ allcols]
    Xcolmap = Dict(c=>findfirst(Xcols, c) for c ∈ allcols)
    ycolmap = Dict(c=>findfirst(ycols, c) for c ∈ allcols)
    idx::AbstractVector{<:Integer} -> _harvest_batch(h, T, Xcolmap, Xwidth, ycolmap, ywidth,
                                                     allcols, alltypes, idx)
end
function harvester{T}(src, Xcols::AbstractVector{Symbol}, ycols::AbstractVector{Symbol},
                      ::Type{T})
    harvester(Harvester(src, Xcols, ycols), T)
end
function harvester{T}(src, Xcols::AbstractVector{Symbol}, ::Type{T})
    harvester(Harvester(src, Xcols), T)
end
export harvester


function batchiter{T}(h::Harvester, idx::AbstractVector{<:Integer}, ::Type{T};
                      batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    batchiter(harvester(h, T), idx, batch_size)
end




