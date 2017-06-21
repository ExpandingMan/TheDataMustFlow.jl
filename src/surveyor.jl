# TODO so far this is very much a work in progress and experimental


#=========================================================================================
    <survey>
=========================================================================================#
mutable struct Survey
    idx::AbstractVector
    catpools::Dict{Int,CategoricalPool}

    Survey() = new(Vector{Int}(), Dict{Int,CategoricalPool}())
end
export Survey
#=========================================================================================
    </survey>
=========================================================================================#


#=========================================================================================
    <surveyor>
=========================================================================================#
struct Surveyor <: AbstractMorphism{Pull}
    s::Any
    schema::Data.Schema

    cols::Vector{Tuple}
    funcs::Vector{Function}

    surveys::Vector{Survey}
    poolcols::Vector{Int}

    function Surveyor(s, sch::Data.Schema, cols::AbstractVector, func::Function,
                      poolcols::Vector{Int}=Int[])
        cols = _handle_col_args(sch, cols)
        new(s, sch, Tuple[cols], Function[func], Survey[], poolcols)
    end
    function Surveyor(s, cols::AbstractVector, func::Function, poolcols::Vector{Int}=Int[])
        Surveyor(s, Data.schema(s), cols, func, poolcols)
    end
end
export Surveyor

getsurvey(sv::Surveyor, n::Integer) = sv.surveys[n]

addsurvey!(sv::Surveyor, surv::Survey) = push!(sv.surveys, surv)
#=========================================================================================
    </surveyor>
=========================================================================================#




#=========================================================================================
    <intermediate constructors>
=========================================================================================#
function _surveyor_pull_cols(cols::AbstractVector, pool_cols::AbstractVector)
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
    function (args::AbstractVector...)
        o = logical_op.((funcs[i].(args[i]) for i ∈ 1:ℓ)...)
        o, Any[args[i][o] for i ∈ poolcolargs]  # fetch categories only for cols we want
    end
end

# TODO for now we are using this simple form. will eventually need a macro to do better stuff
function Surveyor(s, sch::Data.Schema, cols::AbstractVector, filterfuncs::AbstractVector{<:Function};
                  lift_nulls::Bool=true, logical_op::Function=(&),
                  pool_cols::AbstractVector=[])
    cols = _handle_col_args(Vector, sch, cols)
    pool_cols = _handle_col_args(Vector, sch, pool_cols)
    pull_cols, poolcolargs = _surveyor_pull_cols(cols, pool_cols)
    Surveyor(s, sch, pull_cols,
             _combine_surveyor_funcs(filterfuncs, poolcolargs, lift_nulls, logical_op), pool_cols)
end
#=========================================================================================
    </intermediate constructors>
=========================================================================================#


#=========================================================================================
    <basic functions>

    # NOTE: it is still intended that the survey be run on the entire dataset
    # it is not possible to determine length of index vector ahead of time
=========================================================================================#
function surveyor(sv::Surveyor, ::Type{Bool})
    m = morphism(sv)

    function (idx::AbstractVector{<:Integer})
        (b, colsvec), = m(idx)
        b, colsvec
    end
end

# TODO add size estimation for survey idx return
function surveyor(sv::Surveyor; survey_number::Integer=-1)
    if survey_number < 1
        surv = addsurvey!(sv, Survey())[end]
    else
        surv = getsurvey(sv, survey_number)
    end

    m = morphism(sv)

    function (idx::AbstractVector{<:Integer})
        (b, colsvec), = m(idx)
        newidx = find(b) + idx[1] - 1
        append!(surv.idx, idx)
        newidx, colsvec
    end
end
export surveyor
#=========================================================================================
    </basic functions>
=========================================================================================#




