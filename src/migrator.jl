
#=========================================================================================
    <function creators>
=========================================================================================#
_wrap_iter(x) = tuple(x)
_wrap_iter(x::Tuple) = x
_wrap_iter(x::AbstractVector) = x

# these function args are assumed to be in form of morphism output
function migrator(src::Function, snk::Function; index_map::Function=identity)
    idx::AbstractVector{<:Integer} -> snk(index_map.(idx), _wrap_iter(src(idx))...)
end
function migrator(src::AbstractMorphism{PullBack}, snk::AbstractMorphism{PushForward};
                  index_map::Function=identity)
    migrator(morphism(src), morphism(snk), index_map=index_map)
end
function migrator(src_snk::Pair{<:Function,<:Function}; index_map::Function=identity)
    migrator(src_snk[1], src_snk[2], index_map=index_map)
end
function migrator(src_snk::Pair{<:AbstractMorphism{PullBack},<:AbstractMorphism{PushForward}};
                  index_map::Function=identity)
    migrator(src_snk[1], src_snk[2], index_map=index_map)
end

function migrator(src, src_sch::Data.Schema, src_cols::AbstractVector{<:Tuple},
                  src_funcs::AbstractVector{<:Function},
                  snk, snk_sch::Data.Schema, snk_cols::AbstractVector{<:Tuple},
                  snk_funcs::AbstractVector{<:Function}; index_map::Function=identity)
    ϕ = Morphism{PullBack}(src, src_sch, src_cols, src_funcs)
    θ = Morphism{PushForward}(snk, snk_sch, snk_cols, snk_funcs)
    migrator(ϕ, θ, index_map=index_map)
end
function migrator(src, src_cols::AbstractVector{<:Tuple},
                  src_funcs::AbstractVector{<:Function},
                  snk, snk_cols::AbstractVector{<:Tuple},
                  snk_funcs::AbstractVector{<:Function}; index_map::Function=identity)
    ϕ = Morphism{PullBack}(src, src_cols, src_funcs)
    θ = Morphism{PushForward}(src, src_cols, src_funcs)
    migrator(ϕ, θ, index_map=index_map)
end

function migrator(src, src_cols::AbstractVector{Symbol},
                  snk, snk_cols::AbstractVector{Symbol};
                  index_map::Function=identity)
    harvest = harvester(src, Any, src_cols)
    sow! = sower(snk, snk_cols)
    migrator(harvest, sow!, index_map=index_map)
end

function migrator(src, snk, src_cols::AbstractVector{Symbol};
                  name_map::Union{Function,Dict}=identity, index_map::Function=identity)
    name_map = dictfunc(name_map)
    snk_cols = name_map.(src_cols)
    migrator(src, src_cols, snk, snk_cols, index_map=index_map)
end
function migrator(src_snk::Pair, src_cols::AbstractVector{Symbol};
                  name_map::Union{Function,Dict}=identity, index_map::Function=identity)
    migrator(src_snk[1], src_cols, src_snk[2], snk_cols, index_map=index_map)
end

function migrator(src, snk; name_map::Union{Function,Dict}=identity,
                  index_map::Function=identity)
    cols = Symbol.(Data.header(Data.schema(src)))
    migrator(src, snk, cols; name_map=name_map, index_map=index_map)
end
function migrator(src_snk::Pair; name_map::Union{Function,Dict}=identity,
                  index_map::Function=identity)
    migrator(src_snk[1], src_snk[2], name_map=name_map, index_map=index_map)
end
export migrator
#=========================================================================================
    </function creators>
=========================================================================================#


#=========================================================================================
    <iterable creators>
=========================================================================================#
function batchiter(idx::AbstractVector{<:Integer}, args...;
                   index_map::Function=identity,
                   name_map::Union{Function,Dict}=identity,
                   batch_size::Integer=DEFAULT_SOW_BATCH_SIZE)
    m! = migrator(args...; index_map=index_map, name_map=name_map)
    batchiter(m!, idx, batch_size)
end
#=========================================================================================
    </iterable creators>
=========================================================================================#



function migrate!(idx::AbstractVector{<:Integer}, args...;
                  name_map::Union{Function,Dict}=identity,
                  index_map::Function=identity, batch_size::Integer=DEFAULT_SOW_BATCH_SIZE)
    iter = batchiter(idx, args...; index_map=index_map, name_map=name_map,
                     batch_size=batch_size)
    foreach(x -> nothing, iter)
end
export migrate!


