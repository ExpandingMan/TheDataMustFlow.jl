

"""
# Type: `Sower`

***TODO*** Documentation!!!
"""
struct Sower <: AbstractMorphism{PushForward}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}


    function Sower(s, sch::Data.Schema, cols::AbstractVector{<:Tuple},
                         funcs::AbstractVector{<:Function})
        new(s, sch, cols, funcs)
    end
    function Sower(s, cols::AbstractVector{<:Tuple},
                         funcs::AbstractVector{<:Function})
        new(s, Data.schema(s), cols, funcs)
    end

    function Sower(s, sch::Data.Schema, cols::AbstractVector{Symbol},
                         funcs::AbstractVector{<:Function})
        cols = [tuple((sch[string(c)] for c ∈ co)...) for co ∈ cols]
        Sower(s, sch, cols, funcs)
    end
    function Sower(s, cols::AbstractVector{Symbol},
                         funcs::AbstractVector{<:Function})
        Sower(s, Data.schema(s), cols, func)
    end
end
export Sower


#=========================================================================================
    <intermediate constructors>
=========================================================================================#
function Sower(s, sch::Data.Schema, cols::AbstractVector{Tuple})
    Sower(s, sch, cols, Function[identity for i ∈ 1:length(cols)])
end
function Sower(s, cols::AbstractVector{Tuple})
    Sower(s, Data.schema(s), cols)
end

function Sower(s, sch::Data.Schema, cols::AbstractVector{<:Integer})
    Sower(s, sch, Tuple[tuple(cols...)])
end
function Sower(s, cols::AbstractVector{<:Integer})
    Sower(s, Data.schema(s), cols)
end
function Sower(s, sch::Data.Schema, cols::AbstractVector{Symbol})
    Sower(s, sch, colidx(sch, cols))
end
function Sower(s, cols::AbstractVector{Symbol})
    Sower(s, Data.schema(s), cols)
end

Sower(s, cols::Symbol...) = Sower(s, collect(cols))
#=========================================================================================
    </intermediate constructrs>
=========================================================================================#





#=========================================================================================
    <migrating>
=========================================================================================#
function migrator(s::Sower, src, cols::AbstractVector{Symbol};
                  name_map::Dict=Dict(), index_map::Function)

end
export migrator


# this is the iterator for migrating
function batchiter(s::Sower, src, cols::AbstractVector{Symbol},
                   idx::AbstractVector{<:Integer};
                   name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                   index_map::Function=identity)
    m! = migrator(s, src, cols, name_map=name_map, index_map=index_map)
    batchiter(m!, idx, batch_size)
end
function batchiter(s::Sower, src, idx::AbstractVector{<:Integer};
                   name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                   index_map::Function=identity)
    batchiter(s, src, Symbol.(Data.header(data.schema(src))), name_map=name_map,
              index_map=index_map, batch_size=batch_size)
end


# this does the full migration
function migrate!(s::Sower, src, cols::AbstractVector{Symbol},
                  idx::AbstractVector{<:Integer};
                  name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    iter = batchiter(s, src, cols, idx, name_map=name_map, batch_size=batch_size,
                     index_map=index_map)
    foreach(x -> nothing, iter)
end
function migrate!(s::Sower, src, idx::AbstractVector{<:Integer};
                  name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    migrate!(s, src, Symbol.(Data.header(Data.schema(src))), idx, name_map=name_map,
             batch_size=batch_size, index_map=index_map)
end
# these have internal Sower constructors
function migrate!(source_sink::Pair, cols::AbstractVector{Symbol},
                  idx::AbstractVector{<:Integer};
                  name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    s = Sower(source_sink[2], Symbol[])
    migrate!(s, source_sink[1], cols, idx, name_map=name_map, batch_size=batch_size,
             index_map=index_map)
end
function migrate!(source_sink::Pair, idx::AbstractVector{<:Integer};
                  name_map::Dict=Dict(), batch_size::Integer=DEFAULT_SOW_BATCH_SIZE,
                  index_map::Function=identity)
    s = Sower(source_sink[2], Symbol[])
    migrate!(s, source_sink[1], idx, name_map=name_map, batch_size=batch_size,
             index_map=index_map)
end
export migrate!
#=========================================================================================
    </migrating>
=========================================================================================#



#=========================================================================================
    <sowing>
=========================================================================================#
# this is largely a function boundary for efficiency
function _sow_column!{T,To}(s::Sower, ycol::Vector{T}, ::Type{To},
                            idx::AbstractVector{<:Integer}, tocol::Integer)
    streamto!(s.snk, Data.Column, convert(To, ycol), idx, tocol, s.schema)
end
# it is expected that this idx and y are not for the complete dataset
# this is expected to be in an external loop for lots of different y batches
"""
    sower(s::Sower)

Returns a function `s!(idx, y)` which places the contents of `y` into the indices `idx` of
the `Sower`'s sink.
"""
function sower(s::Sower)
    tocols = colidx(s.schema, s.newycols)
    totypes = Data.types(s.schema)
    totypes = Dict(i=>vectortype(totypes[i]) for i ∈ tocols)
    function (idx::AbstractVector{<:Integer}, y::Matrix)
        for col ∈ 1:size(y,2)
            tocol = tocols[col]
            _sow_column!(s, y[:,col], totypes[tocol], idx, tocol)
        end
    end
end
function sower(src, sch::Data.Schema, newycols::AbstractVector{Symbol})
    sower(Sower(src, sch, newycols))
end
sower(src, newycols::AbstractVector{Symbol}) = sower(Sower(src, newycols))
export sower

# note that this doesn't get an iterator because it takes arguments

#=========================================================================================
    </sowing>
=========================================================================================#



