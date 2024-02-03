"""Example of AI voltage sw operation."""
import nidaqmx
import nidaqmx.system

system = nidaqmx.system.System.local()
devices = system.devices

for device in devices:
    # print(f"Device: {device}")
    print(device.ai_physical_chans.channel_names)
    ai_channels = device.ai_physical_chans
    if ai_channels:
        print(f"Analog Input support: Yes")
        print(f"Number of AI channels: {len(ai_channels)}")
    else:
        print(f"Analog Input support: No")

with nidaqmx.Task() as task:

    task.ai_channels.add_ai_voltage_chan("cDAQ1Mod1/ai2:7")
    data = task.read() # print 1 sample per channel
    print(data)
