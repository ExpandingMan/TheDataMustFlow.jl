using DataTables
using DataStreams
using Feather
using TheDataMustFlow
using DataUtils

const filename = "sample.feather"
const filt_batch_size = 2048
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


# create a sink to put data back into
dtypes = [DataType[eltype(dt) for dt ∈ Data.types(src_sch)]; Float32; Float32]
header = [Symbol.(Data.header(src_sch)); :γ; :δ]
sink = DataTable(dtypes, header, nrows)



# create StreamFilter
sfilt = StreamFilter(src, [:Header1, :Header2],
                     Function[i -> (i % 2 == 0), i -> (i % 3 == 0)])
filt = streamfilter(sfilt)

# create Harvester
h = Harvester(src, [:A, :B], Symbol[])
harvest = harvester(h, Float64)

# create Sower
s = Sower(sink, [:γ, :δ])
# for now let's do the migration ahead of time
migrate!(s, src, 1:nrows)
sow! = sower(s)



const idx_chnl = Channel{Vector}(256)
const y_chnl = Channel{Tuple}(128)

# put stuff onto the index channel
for batch ∈ batchiter(1:100, filt_batch_size)
    @schedule begin
        println("filling idx channel...")
        put!(idx_chnl, filt(batch))
    end
end

function transfer_harvest()
    while true
        println("filling y_channel")
        idx = take!(idx_chnl)
        X, _ = harvest(idx)
        y = -X
        put!(y_chnl, (idx, y))
        println("done filling y channel")
    end
end

for i ∈ 1:16
    @schedule transfer_harvest()
end

function do_sow()
    while true
        println("sowing...")
        idx, y = take!(y_chnl)
        sow!(idx, y)
    end
end

for i ∈ 1:16
    @schedule do_sow()
end





