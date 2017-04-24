using DataTables
using DataStreams
using Feather
using TheDataMustFlow
using DataUtils

const filename = "sample.feather"
const batch_size = 1024

src = Feather.Source(filename)
src_sch = Data.schema(src)
nrows = size(src, 1)
# data = featherRead(filename)
# nrows = size(data,1)
# data[:γ] = Nullable{Float32}()
# data[:δ] = Nullable{Float32}()
# src = data
# sink = data


# create StreamFilter
sfilter = StreamFilter(src, [:Header1, :Header2],
                       Function[i -> (i % 2 == 0), i -> (i % 3 == 0)])
# collect all valid indices
idx = filterall(sfilter, 1:nrows)

# construct Harvester
h = Harvester(src, [:A, :B], Symbol[])
harvest = harvester(h, Float64)

# create a sink to put data back into
dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src_sch)]; Float32; Float32]
header = [Symbol.(Data.header(src_sch)); :γ; :δ]
sink = DataTable(dtypes, header, nrows)

# create Sower
s = Sower(sink, [:γ, :δ])
sow! = sower(s)
# migrate everything
migrate!(s, src, 1:nrows)

# main loop
@time for sidx ∈ batchiter(idx, batch_size)
    X, y = harvest(sidx)
    y = -X
    sow!(sidx, y)
end

