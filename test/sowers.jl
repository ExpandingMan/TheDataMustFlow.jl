using DataTables
using DataStreams
using Feather
using TheDataMustFlow
using DataUtils

const filename = "sample.feather"
const batch_size = 1024

# src = Feather.Source(filename)
# src_sch = Data.schema(src)
# nrows = size(src, 1)
data = featherRead(filename)
nrows = size(data,1)
data[:γ] = Nullable{Float32}()
data[:δ] = Nullable{Float32}()
src = data
sink = data


filter = StreamFilter(src, [:Header1, :Header2],
                      Function[i -> (i % 2 == 0), i -> (i % 3 == 0)])
idx = index(filter, 1:nrows)
h = Harvester(src, [:A, :B], Symbol[])

# create a sink to put data back into
# dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src_sch)]; Float32; Float32]
# header = [Symbol.(Data.header(src_sch)); :γ; :δ]
# sink = DataTable(dtypes, header, nrows)

s = Sower(sink, [:γ, :δ], h)

# migrate!(s, 1:nrows)

for (X,_,sidx) ∈ harvest(h, idx, Float64)
    y = -X
    sow!(s, sidx, y)
end

