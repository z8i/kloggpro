
Basis for an Integration of TFA KlimaLogg pro into Home Assistant

Based on this https://github.com/matthewwall/weewx-klimalogg

Just took the kl.py and in a first step tried to get it work without weewx.
-> this works :-)
run klimalogg.py and get the values from the base station.

I am using a Raspberry Pi with raspbian. To get usb access working
(Error: The device has no langid (permission issue, no string descriptors supported or device error))
i followed these instructions https://www.raspberrypi.org/forums/viewtopic.php?t=186839
(my user is named 'picoder')
```bash
sudo adduser picoder plugdev

vim /etc/udev/rules.d/50-usb-perms.rules

SUBSYSTEM=="usb", ATTRS{idVendor}=="6666", ATTRS{idProduct}=="5555", GROUP="plugdev", MODE="0660"

lsusb
# should show the klimlogg receiver, here bus 001 device 004
ls -l /dev/bus/usb/001/004
# expected output would be
crw-rw-rw- 1 root plugdev 189, 3 Dec 21 21:52 /dev/bus/usb/001/004
# now group plugdev can access the device
```

