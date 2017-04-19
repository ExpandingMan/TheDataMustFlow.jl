
# TODO for now everything is one row at a time, at some point we must do blocks

# this should be able to handle sources with nulls
struct StreamFilter <: AbstractStreamFilter
    src::Any
    schema::Data.Schema

    filtercols::Vector{Symbol}

    filterfuncs::Vector{Function}


    function StreamFilter(src, sch::Data.Schema, filtercols::AbstractVector{Symbol},
                          filterfuncs::AbstractVector{Function})
        if length(filtercols) ≠ length(filterfuncs)
            throw(ArgumentError("Must supply same number of filter functions as columns."))
        end
        new(src, sch, convert(Vector{Symbol}, filtercols),
            convert(Vector{Function}, filterfuncs))
    end

    function StreamFilter(src, filtercols::AbstractVector{Symbol},
                          filterfuncs::AbstractVector{Function})
        StreamFilter(src, Data.schema(src), filtercols, filterfuncs)
    end
end
export StreamFilter


#=========================================================================================
    <interface>
=========================================================================================#
colidx(f::StreamFilter) = colidx(f.schema, f.filtercols)

function rowtype(f::StreamFilter)
    header = Data.header(f.schema)
    dtypes = Data.types(f.schema)
    filtercols = String[string(c) for c ∈ f.filtercols]
    idx = findin(header, filtercols)
    Tuple{dtypes[idx]...}
end


function _index_batch(f::StreamFilter, cols::AbstractVector{<:Integer},
                      ctypes::AbstractVector{DataType},
                      batch_idx::AbstractVector{<:Integer})
    allcols = Vector{AbstractVector{Bool}}(length(f.filtercols))
    for i ∈ 1:length(f.filtercols)
        allcols[i] = sift(f.src, f.filterfuncs[i], ctypes[i], cols[i],
                          batch_idx)
    end
    mask = .&(allcols...)
    find(mask)
end
function index{T}(f::StreamFilter, idx::AbstractVector{T};
                  batch_size::Integer=DEFAULT_FILTER_BATCH_SIZE)
    cols = colidx(f)
    ctypes = coltypes(f.schema, cols)
    o = Vector{T}()  # it's impossible to know the length of this a priori
    for batch_idx ∈ batchiter(idx, batch_size)
        append!(o, _index_batch(f, cols, ctypes, batch_idx))
    end
    o
end
export index
#=========================================================================================
    <interface>
=========================================================================================#


