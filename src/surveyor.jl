# TODO so far this is very much a work in progress and experimental


# type for holding survey output
struct Survey
    iter::AbstractVector
    catpools::Dict{Int,CategoricalPool}
end



struct Surveyor <: AbstractMorphism{Pull}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    poolcols::Dict{Int,CategoricalPool}

    function Surveyor(s, sch::Data.Schema, cols::AbstractVector, func::Function)
        cols = _handle_col_args(sch, cols)
        new(s, sch, Tuple[cols], Function[func])
    end
    function Surveyor(s, cols::AbstractVector, func::Function)
        Surveyor(s, Data.schema(s), cols, func)
    end
end


#=========================================================================================
    <intermediate constructors>
=========================================================================================#
function _surveyor_pull_cols(cols::AbstractVector{<:Integer},
                             pool_cols::AbstractVector{<:Integer})
    extra_cols = [c for c ∈ pool_cols if c ∉ cols]
    allcols = Int[cols; extra_cols]
    poolargs = findin(allcols, pool_cols)
    allcols, poolargs
end

function _nolift_func(f::Function)
    g(v::AbstractArray) = f.(v)

    function g(v::NullableArray)
        o = BitArray(length(v))
        for i ∈ 1:length(v)
            o[i] = f(v[i])
        end
        o
    end

    g
end

function _combine_surveyor_funcs(funcs::AbstractVector{<:Function},
                                 poolcolargs::AbstractVector{<:Integer},
                                 lift::Bool, logical_op::Function=(&))
    ℓ = length(funcs)
    if !lift
        funcs = _nolift_func.(funcs)
    end
    function g(args::AbstractVector...)
        logical_op.((funcs[i].(args[i]) for i ∈ 1:ℓ)...), [args[i] for i ∈ poolcolargs]
    end
end

# TODO for now we are using this simple form. will eventually need a macro to do better stuff
function Surveyor(s, sch::Data.Schema, cols::AbstractVector, filterfuncs::AbstractVector{<:Function};
                  lift_nulls::Bool=true, logical_op::Function=(&),
                  pool_cols::AbstractVector=[])
    cols = _handle_col_args(sch, cols)
    pool_cols = _handle_col_args(sch, pool_cols)
    pull_cols, poolcolargs = _surveyor_pull_cols(cols, pool_cols)
    Surveyor(s, sch, pull_cols,
             _combine_surveyor_funcs(filterfuncs, poolcolargs, lift_nulls, logical_op))
end
#=========================================================================================
    </intermediate constructors>
=========================================================================================#


#=========================================================================================
    <basic functions>
=========================================================================================#
function surveyor(sv::Surveyor, ::Type{Bool})
    m = morphism(sv)
    function (idx::AbstractVector{<:Integer})
        (b,), colsvec = m(idx)
    end
end
#=========================================================================================
    </basic functions>
=========================================================================================#




