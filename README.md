# kloggpro

This is a library to query "TFA KlimaLogg pro" weather stations via its
included USB transceiver. It is used for an Integration into Home
Assistant.

## Acknowledgement

This is derived from the code of [weex-klimalogg by matthewwall](https://github.com/matthewwall/weewx-klimalogg).
Thanks alot to the authors of that project!

## Status experimental!

`kloggpro` can be installed via PyPI on Raspberry Pi OS.

It needs usb access to work properly, maybe you need to grant usb access
to the USB-Transceiver by 
* adding the user to plugdev group  
  `sudo adduser <username> plugdev` 
* add following rule to `/etc/udev/rules.d/50-usb-perms.rules`:  
  `SUBSYSTEM=="usb", ATTRS{idVendor}=="6666", ATTRS{idProduct}=="5555", GROUP="plugdev", MODE="0660"`

To check for success: 
```bash
  lsusb 
  Bus 001 Device 004: ID 6666:5555 Prototype product Vendor ID 
  # should show the klimlogg receiver, here bus 001 device 004 
  ls -l /dev/bus/usb/001/004 
  # expected output would be 
  crw-rw-rw- 1 root plugdev 189, 3 Dec 21 21:52 /dev/bus/usb/001/004
  # now group plugdev can access the device and everything should work
```
(I had an error: `The device has no langid (permission issue, no string
descriptors supported or device error)`, so i followed [these
instructions](https://www.raspberrypi.org/forums/viewtopic.php?t=186839))

## Usage

After successful installation and usb configuration start python and  
`import kloggpro.klimalogg`  
Now you can create a KlimaLoggDriver-Object  
`kldr = KlimaLoggDriver()`  
To start communication, you need to clear wait state  
`kldr.clear_wait_at_start()`

Now it is possible to either access the current values object directly  
`kldr._service.current.values['Temp0']`  
or you can receive packets:
```python
for packet in kldr.genLoopPackets():
    print("Time: {} - {}".format(packet['dateTime'], packet))
    time.sleep(5)
```