#=========================================================================================
    DataStreams Interface

        Here we re-implement the datastreams interface for wrapper types within
        TheDataMustFlow.
=========================================================================================#


#=========================================================================================
    <harvester>
=========================================================================================#
Data.isdone(h::Harvester, row, col) = Data.isdone(h.src, row, col)
Base.size(h::Harvester) = Base.size(h.src)
Data.schema(h::Harvester) = Data.schema(h.src)
Data.schema{T<:Data.StreamType}(h::Harvester, ::Type{T}) = Data.schema(h.src, T)
function Data.streamtype{T<:Data.StreamType}(h::Harvester, ::Type{T})
    Data.streamtype(typeof(h.src), T)
end

function Data.streamfrom{T}(h::Harvester, ::Type{Data.Field}, ::Type{T}, row, col)
    Data.streamfrom(h.src, Data.Field, T, row, col)
end
function Data.streamfrom{T}(h::Harvester, ::Type{Data.Column}, ::Type{T}, col)
    Data.streamfrom(h.src, Data.Column, T, col)
end
#=========================================================================================
    </harvester>
=========================================================================================#


#=========================================================================================
    <sower>
=========================================================================================#
function Sink{T<:Data.StreamType}(snk, sch::Data.Schema, ::Type{T}, append::Bool,
                                  ref::Vector{UInt8})
    Sower(snk, sch, Symbol[])
end

Data.streamtypes(s::Sower) = Data.streamtypes(typeof(s.snk))

function Data.streamto!{T}(s::Sower, ::Type{Data.Field}, val::T, row, col)
    Data.streamto!(s.snk, Data.Field, val, row, col)
end
function Data.streamto!{T}(s::Sower, ::Type{Data.Field}, val::T, row, col, sch::Data.Schema)
    Data.streamto!(s.snk, Data.Field, val, row, col, sch)
end

function Data.streamto!{T}(s::Sower, ::Type{Data.Column}, column::T, row, col)
    Data.streamto!(s.snk, Data.Column, column, row, col)
end
function Data.streamto!{T}(s::Sower, ::Type{Data.Column}, column::T, row, col,
                           sch::Data.Schema)
    Data.streamto!(s.snk, Data.Column, column, row, col, sch)
end
#=========================================================================================
    <sower>
=========================================================================================#


