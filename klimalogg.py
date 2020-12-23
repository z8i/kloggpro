# TFA KlimaLogg Driver for an Integration to Home Assistant
#
# made in 2020 by z8i
#
# All this is based on:
# TFA KlimaLogg driver for weewx
#
# Copyright 2015-2020 Luc Heijst, Matthew Wall
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.
#
# See http://www.gnu.org/licenses/
#
# The driver logic is adapted from the ws28xx driver for LaCrosse 2800 and
# TFA Primus and Opus weather stations.
#
# Thanks to Michael Schulze for making the sensor map dynamic.
#
# Also many thanks to our testers, in special Boris Smeds and Raffael Boesch.
# Without their help we couldn't have written this driver.



from datetime import datetime
import random
import sys
import threading
import time
import traceback
import usb
from io import StringIO

DRIVER_NAME = 'KlimaLogg'
DRIVER_VERSION = '1.4.2'
PRESS_USB = "press the USB button to start communication"

stn_dict = { #stn_dict originates from weex.drivers.AbstractDevice
    'vendor_id': 0x6666,
    'product_id': 0x5555,
    'transceiver_frequency': 'EU'
}


ACTION_GET_HISTORY = 0x00
ACTION_REQ_SET_TIME = 0x01
ACTION_REQ_SET_CONFIG = 0x02
ACTION_GET_CONFIG = 0x03    #in use!
ACTION_GET_CURRENT = 0x04
ACTION_SEND_CONFIG = 0x20
ACTION_SEND_TIME = 0x60

RESPONSE_DATA_WRITTEN = 0x10
RESPONSE_GET_CONFIG = 0x20
RESPONSE_GET_CURRENT = 0x30
RESPONSE_GET_HISTORY = 0x40
RESPONSE_REQUEST = 0x50  # the group of all 0x5x requests
RESPONSE_REQ_READ_HISTORY = 0x50
RESPONSE_REQ_FIRST_CONFIG = 0x51
RESPONSE_REQ_SET_CONFIG = 0x52
RESPONSE_REQ_SET_TIME = 0x53

HI_01MIN = 0
HI_05MIN = 1
HI_10MIN = 2
HI_15MIN = 3
HI_30MIN = 4
HI_01STD = 5
HI_02STD = 6
HI_03STD = 7
HI_06STD = 8

history_intervals = {
    HI_01MIN: 1,
    HI_05MIN: 5,
    HI_10MIN: 10,
    HI_15MIN: 15,
    HI_30MIN: 30,
    HI_01STD: 60,
    HI_02STD: 120,
    HI_03STD: 180,
    HI_06STD: 360}

# frequency standards and their associated transmission frequencies
frequencies = {
    'US': 905000000,
    'EU': 868300000,
}

# flags for enabling/disabling debug verbosity
DEBUG_COMM = 2
DEBUG_CONFIG_DATA = 0  #in use"
DEBUG_WEATHER_DATA = 0
DEBUG_HISTORY_DATA = 0
DEBUG_DUMP_FORMAT = 'auto'


KL_SENSOR_MAP = {
    'temp0':          'Temp0',
    'humidity0':      'Humidity0',
    'temp1':          'Temp1',
    'humidity1':      'Humidity1',
    'temp2':          'Temp2',
    'humidity2':      'Humidity2',
    'temp3':          'Temp3',
    'humidity3':      'Humidity3',
    'temp4':          'Temp4',
    'humidity4':      'Humidity4',
    'temp5':          'Temp5',
    'humidity5':      'Humidity5',
    'temp6':          'Temp6',
    'humidity6':      'Humidity6',
    'temp7':          'Temp7',
    'humidity7':      'Humidity7',
    'temp8':          'Temp8',
    'humidity8':      'Humidity8',
    'rxCheckPercent': 'SignalQuality',
    'batteryStatus0': 'BatteryStatus0',
    'batteryStatus1': 'BatteryStatus1',
    'batteryStatus2': 'BatteryStatus2',
    'batteryStatus3': 'BatteryStatus3',
    'batteryStatus4': 'BatteryStatus4',
    'batteryStatus5': 'BatteryStatus5',
    'batteryStatus6': 'BatteryStatus6',
    'batteryStatus7': 'BatteryStatus7',
    'batteryStatus8': 'BatteryStatus8',
}



import logging
log = logging.getLogger(__name__)

def logdbg(msg):
    log.debug(msg)
    print(msg)

def loginf(msg):
    log.info(msg)
    print(msg)
    
def logtee(msg):
    loginf(msg)
    print("%s\r" % msg)

def logerr(msg):
    log.error(msg)
    print(msg)

def logconsole(): #Hiermit kann die Konsole als Logging-Anzeige gesetzt werden
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)

def bytes_to_addr(a, b, c):
    return (((a << 8) | b) << 8) | c

def addr_to_index(addr):
    return (addr - 0x070000) / 32

def index_to_addr(idx):
    return 32 * idx + 0x070000

def get_datum_diff(v, np, ofl):
    if abs(np - v) < 0.001 or abs(ofl - v) < 0.001:
        return None
    return v

def calc_checksum(buf, start, end=None):
    if end is None:
        end = len(buf)
    cs = 0
    for i in range(start, end):
        cs += buf[i]
    return cs

def get_index(idx):
    if idx < 0:
        return idx + KlimaLoggDriver.max_records
    elif idx >= KlimaLoggDriver.max_records:
        return idx - KlimaLoggDriver.max_records
    return idx

def tstr_to_ts(tstr):
    try:
        return int(time.mktime(time.strptime(tstr, "%Y-%m-%d %H:%M:%S")))
    except (OverflowError, ValueError, TypeError):
        pass
    return None

# The following classes and methods are adapted from the implementation by
# eddie de pieri, which is in turn based on the HeavyWeather implementation.
class BadResponse(Exception):
    """raised when unexpected data found in frame buffer"""
    pass

class UnknownDeviceId(Exception):
    """raised when unknown device ID found in frame buffer"""
    pass

class DataWritten(Exception):
    """raised when message 'data written' in frame buffer"""
    pass

# NP - not present
# OFL - outside factory limits
class SensorLimits:
    temperature_offset = 40.0
    temperature_NP = 81.1
    temperature_OFL = 136.0
    humidity_NP = 110.0
    humidity_OFL = 121.0

class AX5051RegisterNames:
    REVISION     = 0x0
    SCRATCH      = 0x1
    POWERMODE    = 0x2
    XTALOSC      = 0x3
    FIFOCTRL     = 0x4
    FIFODATA     = 0x5
    IRQMASK      = 0x6
    IFMODE       = 0x8
    PINCFG1      = 0x0C
    PINCFG2      = 0x0D
    MODULATION   = 0x10
    ENCODING     = 0x11
    FRAMING      = 0x12
    CRCINIT3     = 0x14
    CRCINIT2     = 0x15
    CRCINIT1     = 0x16
    CRCINIT0     = 0x17
    FREQ3        = 0x20
    FREQ2        = 0x21
    FREQ1        = 0x22
    FREQ0        = 0x23
    FSKDEV2      = 0x25
    FSKDEV1      = 0x26
    FSKDEV0      = 0x27
    IFFREQHI     = 0x28
    IFFREQLO     = 0x29
    PLLLOOP      = 0x2C
    PLLRANGING   = 0x2D
    PLLRNGCLK    = 0x2E
    TXPWR        = 0x30
    TXRATEHI     = 0x31
    TXRATEMID    = 0x32
    TXRATELO     = 0x33
    MODMISC      = 0x34
    FIFOCONTROL2 = 0x37
    ADCMISC      = 0x38
    AGCTARGET    = 0x39
    AGCATTACK    = 0x3A
    AGCDECAY     = 0x3B
    AGCCOUNTER   = 0x3C
    CICDEC       = 0x3F
    DATARATEHI   = 0x40
    DATARATELO   = 0x41
    TMGGAINHI    = 0x42
    TMGGAINLO    = 0x43
    PHASEGAIN    = 0x44
    FREQGAIN     = 0x45
    FREQGAIN2    = 0x46
    AMPLGAIN     = 0x47
    TRKFREQHI    = 0x4C
    TRKFREQLO    = 0x4D
    XTALCAP      = 0x4F
    SPAREOUT     = 0x60
    TESTOBS      = 0x68
    APEOVER      = 0x70
    TMMUX        = 0x71
    PLLVCOI      = 0x72
    PLLCPEN      = 0x73
    PLLRNGMISC   = 0x74
    AGCMANUAL    = 0x78
    ADCDCLEVEL   = 0x79
    RFMISC       = 0x7A
    TXDRIVER     = 0x7B
    REF          = 0x7C
    RXMISC       = 0x7D



class CommunicationService(object):

    def __init__(self, first_sleep, values, max_records=51200, batch_size=100):
        logdbg('CommunicationService.init')

        self.first_sleep = first_sleep
        self.values = values
        self.reg_names = dict()
        self.hid = Transceiver()
        self.transceiver_settings = TransceiverSettings()
        self.last_stat = LastStat()
        self.station_config = StationConfig()
        self.current = CurrentData()
        self.comm_mode_interval = 8
        self.config_serial = None # optionally specified serial number
        self.logger_id = 0 # the default logger id
        self.transceiver_present = False
        self.registered_device_id = None

        self.firstSleep = 1
        self.nextSleep = 1
        self.pollCount = 0

        self.running = False
        self.child = None
        self.thread_wait = 60.0  # seconds

        self.command = None
        self.history_cache = HistoryCache()
        self.ts_last_rec = 0
        self.records_skipped = 0

        self.max_records = max_records
        self.batch_size = batch_size

    def buildFirstConfigFrame(self, cs):
        logdbg('buildFirstConfigFrame: cs=%04x' % cs)
        newlen = 11
        newbuf = [0] * newlen
        historyAddress = 0x010700
        newbuf[0] = 0xF0
        newbuf[1] = 0xF0
        newbuf[2] = 0xFF
        newbuf[3] = ACTION_GET_CONFIG
        newbuf[4] = 0xFF
        newbuf[5] = 0xFF
        newbuf[6] = 0x80  # TODO: not known what this means; (we don't use the high part of self.comm_mode_interval here)
        newbuf[7] = self.comm_mode_interval & 0xFF
        newbuf[8] = (historyAddress >> 16) & 0xFF
        newbuf[9] = (historyAddress >> 8) & 0xFF
        newbuf[10] = (historyAddress >> 0) & 0xFF
        return newlen, newbuf

    def buildConfigFrame(self, buf):
        logdbg("buildConfigFrame")
        changed, cfgbuf = self.station_config.testConfigChanged()
        if changed:
            newlen = 125  # 0x7D
            newbuf = [0] * newlen
            newbuf[0] = buf[0]
            newbuf[1] = buf[1]
            newbuf[2] = buf[2]
            newbuf[3] = ACTION_SEND_CONFIG # 0x20 # change this value if we won't store config
            newbuf[4] = buf[4]
            for i in range(5, newlen):
                newbuf[i] = cfgbuf[i]
            if DEBUG_CONFIG_DATA > 2:
                self.hid.dump('OutBuf', newbuf, fmt='long', length=newlen)
        else:  # current config not up to date; do not write yet
            newlen = 0
            newbuf = [0]
        return newlen, newbuf

    @staticmethod
    def buildTimeFrame(buf, cs):
        logdbg("buildTimeFrame: cs=%04x" % cs)

        tm = time.localtime()

        # d5 00 0d 01 07 00 60 1a b1 25 58 21 04 03 41 01 
        #           0  1  2  3  4  5  6  7  8  9 10 11 12
        newlen = 13
        newbuf = [0] * newlen
        newbuf[0] = buf[0]
        newbuf[1] = buf[1]
        newbuf[2] = buf[2]
        newbuf[3] = ACTION_SEND_TIME  # 0x60
        newbuf[4] = (cs >> 8) & 0xFF
        newbuf[5] = (cs >> 0) & 0xFF
        newbuf[6] = (tm[5] % 10) + 0x10 * (tm[5] // 10)  # sec
        newbuf[7] = (tm[4] % 10) + 0x10 * (tm[4] // 10)  # min
        newbuf[8] = (tm[3] % 10) + 0x10 * (tm[3] // 10)  # hour
        # mo=0, tu=1, we=2, th=3, fr=4, sa=5, su=6  # DayOfWeek format of ws28xx devices
        # mo=1, tu=2, we=3, th=4, fr=5, sa=6, su=7  # DayOfWeek format of klimalogg devices
        DayOfWeek = tm[6]+1       # py  from 1 - 7 - 1=Mon
        newbuf[9] = DayOfWeek % 10 + 0x10 * (tm[2] % 10)            # day_lo   + DoW
        newbuf[10] = (tm[2] // 10) + 0x10 * (tm[1] % 10)           # month_lo + day_hi
        newbuf[11] = (tm[1] // 10) + 0x10 * ((tm[0] - 2000) % 10)  # year-lo  + month-hi
        newbuf[12] = (tm[0] - 2000) // 10                           # not used + year-hi
        return newlen, newbuf

    def buildACKFrame(self, buf, action, cs, hidx=None):
        if DEBUG_COMM > 1:
            logdbg("buildACKFrame: action=%x cs=%04x historyIndex=%s" %
                   (action, cs, hidx))

        comInt = self.comm_mode_interval

        # When last weather is stale, change action to get current weather
        # This is only needed during long periods of history data catchup
        if self.command == ACTION_GET_HISTORY:
            now = int(time.time())
            age = now - self.last_stat.last_weather_ts
            # Morphing action only with GetHistory requests, 
            # and stale data after a period of twice the CommModeInterval,
            # but not with init GetHistory requests (0xF0)
            if (action == ACTION_GET_HISTORY and
                age >= (comInt + 1) * 2 and buf[1] != 0xF0):
                if DEBUG_COMM > 0:
                    logdbg('buildACKFrame: morphing action'
                           ' from %d to 5 (age=%s)' % (action, age))
                action = ACTION_GET_CURRENT

        if hidx == 0xFFFF:
            # At first config preset the address with DeviceId and logger_id
            haddr = (self.getDeviceID() << 8) + int(self.logger_id)
            logdbg('buildACKFrame: first config haddr preset to deviceID and logger_id 0x%06x' % haddr)
        else:
            if hidx is None:
                if self.last_stat.latest_history_index is not None:
                    hidx = self.last_stat.latest_history_index
            if hidx is None or hidx < 0 or hidx >= KlimaLoggDriver.max_records:
                # If no hidx is present yet, preset haddr with 0xffffff
                haddr = 0xFFFFFF
                logdbg('buildACKFrame: no known haddr; preset with 0x%06x' % haddr)
            else:
                haddr = index_to_addr(hidx)
        if DEBUG_COMM > 1:
            logdbg('buildACKFrame: idx: {} addr: 0x{:04x}'.format(hidx, int(haddr)))

        # d5 00 0b f0 f0 ff 03 ff ff 80 03 01 07 00
        #           0  1  2  3  4  5  6  7  8  9 10
        newlen = 11
        newbuf = [0] * newlen
        newbuf[0] = buf[0]
        newbuf[1] = buf[1]
        newbuf[2] = buf[2]
        newbuf[3] = action & 0xF
        newbuf[4] = (cs >> 8) & 0xFF
        newbuf[5] = (cs >> 0) & 0xFF
        newbuf[6] = 0x80  # TODO: not known what this means
        newbuf[7] = comInt & 0xFF
        newbuf[8] = (int(haddr) >> 16) & 0xFF
        newbuf[9] = (int(haddr) >> 8) & 0xFF
        newbuf[10] = (int(haddr) >> 0) & 0xFF
        return newlen, newbuf

    def handleConfig(self, length, buf):
        logdbg('handleConfig: %s' % self.timing())
        if DEBUG_CONFIG_DATA > 2:
            self.hid.dump('InBuf', buf, fmt='long', length=length)
        self.station_config.read(buf)
        if DEBUG_CONFIG_DATA > 1:
            self.station_config.to_log()
        now = int(time.time())
        self.last_stat.update(seen_ts=now,
                              quality=(buf[4] & 0x7f), 
                              config_ts=now)
        cs = buf[124] | (buf[123] << 8)
        self.setSleep(self.first_sleep, 0.010)
        return self.buildACKFrame(buf, ACTION_GET_HISTORY, cs)

    def handleCurrentData(self, length, buf):
        if DEBUG_WEATHER_DATA > 1:
            logdbg('handleCurrentData: %s' % self.timing())

        now = int(time.time())

        # update the weather data cache if stale
        age = now - self.last_stat.last_weather_ts
        if age >= self.comm_mode_interval:
            if DEBUG_WEATHER_DATA > 2:
                self.hid.dump('CurWea', buf, fmt='long', length=length)
            data = CurrentData()
            data.read(buf)
            self.current = data
            if DEBUG_WEATHER_DATA > 0:
                data.to_log()
        else:
            if DEBUG_WEATHER_DATA > 1:
                logdbg('new weather data within %s; skip data; ts=%s' % 
                       (age, now))

        # update the connection cache
        self.last_stat.update(seen_ts=now,
                              quality=(buf[4] & 0x7f),
                              weather_ts=now)

        cs = buf[6] | (buf[5] << 8)
        self.station_config.setSensorText(self.values)
        changed, cfgbuf = self.station_config.testConfigChanged()
        inBufCS = self.station_config.getInBufCS()
        if inBufCS == 0 or inBufCS != cs:
            # request for a get config
            logdbg('handleCurrentData: inBufCS of station does not match')
            self.setSleep(self.first_sleep, 0.010)
            newlen, newbuf = self.buildACKFrame(buf, ACTION_GET_CONFIG, cs)
        elif changed:
            # Request for a set config
            logdbg('handleCurrentData: outBufCS of station changed')
            self.setSleep(self.first_sleep, 0.010)
            newlen, newbuf = self.buildACKFrame(buf, ACTION_REQ_SET_CONFIG, cs)
        else:
            # Request for either a history message or a current weather message
            # In general we don't use ACTION_GET_CURRENT to ask for a current
            # weather  message; they also come when requested for
            # ACTION_GET_HISTORY. This we learned from the Heavy Weather Pro
            # messages (via USB sniffer).
            self.setSleep(self.first_sleep, 0.010)
            newlen, newbuf = self.buildACKFrame(buf, ACTION_GET_HISTORY, cs)
        return newlen, newbuf

    # timestamp of record with time 'None'
    TS_1900 = tstr_to_ts(str(datetime(1900, 1, 1, 0, 0)))

    # initially the clock of the KlimaLogg station starts at 1-jan-2010,
    # so skip all records elder than 1-jul-2010
    # eldest valid timestamp for history record
    TS_2010_07 = tstr_to_ts(str(datetime(2010, 7, 1, 0, 0)))

    def handleHistoryData(self, length, buf):
        if DEBUG_HISTORY_DATA > 1:
            logdbg('handleHistoryData: %s' % self.timing())

        now = int(time.time())
        self.last_stat.update(seen_ts=now,
                              quality=(buf[4] & 0x7f),
                              history_ts=now)

        data = HistoryData()
        data.read(buf)
        if DEBUG_HISTORY_DATA > 1:
            data.to_log()

        cs = buf[6] | (buf[5] << 8)
        latestAddr = bytes_to_addr(buf[7], buf[8], buf[9])
        thisAddr = bytes_to_addr(buf[10], buf[11], buf[12])
        latestIndex = addr_to_index(latestAddr)
        thisIndex = addr_to_index(thisAddr)

        tsPos1 = tstr_to_ts(str(data.values['Pos1DT']))
        tsPos2 = tstr_to_ts(str(data.values['Pos2DT']))
        tsPos6 = tstr_to_ts(str(data.values['Pos6DT']))
        if tsPos1 == self.TS_1900:
            # the first history record has date-time 1900-01-01 00:00:00
            # use the time difference with the second message
            tsFirstRec = tsPos2
        else:
            tsFirstRec = tsPos1
        if tsFirstRec is None or tsFirstRec == self.TS_1900:
            timeDiff = 0
        else:
            timeDiff = abs(now - tsFirstRec)

        # FIXME: what if we do not have config data yet?
        cfg = self.getConfigData().as_dict()
        dcfOn = 'OFF' if int(cfg['settings']) & 0x4 == 0 else 'ON'

        # check for an actual history record (tsPos1 == tsPos2) with valid
        # timestamp (tsPos1 != TS_1900)
        # Take in account that communication might be stalled for 3 minutes during DCF reception and sensor scanning
        # if history date/time differs more than 5 min from now then
        # reqSetTime and initiate alarm
        if data.values['Pos1Alarm'] == 0 and data.values['Pos6Alarm'] == 0:
            # both records are history records
            if tsPos1 == tsPos6 and tsPos1 != self.TS_1900:
                if timeDiff > 300:
                    self.station_config.setAlarmClockOffset()  # set Humidity0Min value to 99
                    logerr('ERROR: DCF: %s; dateTime history record %s differs %s seconds from dateTime server; please check and set set the clock of your station' %
                           (dcfOn, thisIndex, timeDiff))
                    logerr('ERROR: tsPos1: %s, tsPos2: %s' % (tsPos1, tsPos6))
                else:
                    self.station_config.resetAlarmClockOffset()  # set Humidity0Min value to 20
                    if timeDiff > 30:
                        logdbg('DCF = %s; dateTime history record %s differs %s seconds from dateTime server' %
                               (dcfOn, thisIndex, timeDiff))

        # initially the first buffer presented is 6, in fact it starts at 0,
        # which has date None, so we start at 1
        if thisIndex == 6 and latestIndex > 12:
            thisIndex = 1
        nrec = get_index(latestIndex - thisIndex)
        logdbg('handleHistoryData: time=%s this=%d (0x%04x) latest=%d (0x%04x) nrec=%d' %
               (data.values['Pos1DT'],
                thisIndex, thisAddr, latestIndex, latestAddr, nrec))

        # track the latest history index
        self.last_stat.last_history_index = thisIndex
        self.last_stat.latest_history_index = latestIndex

        nextIndex = None
        if self.command == ACTION_GET_HISTORY:
            if self.history_cache.start_index is None:
                if self.history_cache.num_rec > 0:
                    logtee('handleHistoryData: request for %s records' %
                           self.history_cache.num_rec)
                    nreq = self.history_cache.num_rec
                else:
                    if self.history_cache.since_ts > 0:
                        logtee('handleHistoryData: request records since {}'.format(self.history_cache.since_ts))
                        span = int(time.time()) - self.history_cache.since_ts
                        if cfg['history_interval'] is not None:
                            arcint = 60 * history_intervals.get(cfg['history_interval'])
                        else:
                            arcint = 60 * 15  # use the typical history interval of 15 min if interval not known yet
                        # FIXME: this assumes a constant archive interval for
                        # all records in the station history
                        nreq = int(span / arcint) + 5  # FIXME: punt 5
                        if nrec > 0 and nreq > nrec:
                            loginf('handleHistoryData: too many records requested (%d), clipping to number stored (%d)' %
                                  (nreq, nrec))
                            nreq = nrec
                    else:
                        loginf('handleHistoryData: no start date known (empty database), use number stored (%d)' % nrec)
                        nreq = nrec
                # limit number of history records that will be read
                logdbg('handleHistoryData: nreq=%s' % nreq)
                if nreq > self.max_records:
                    nreq = self.max_records
                    loginf('Number of history records limited to: %s' % nreq)
                if nreq >= KlimaLoggDriver.max_records:
                    nrec = KlimaLoggDriver.max_records - 1
                idx = get_index(latestIndex - nreq)
                self.history_cache.start_index = idx
                self.history_cache.next_index = idx
                self.last_stat.last_history_index = idx
                self.history_cache.num_outstanding_records = nreq
                logdbg('handleHistoryData: start_index=%s'
                       ' num_outstanding_records=%s' % (idx, nreq))
                nextIndex = idx
                self.records_skipped = 0
                self.ts_last_rec = 0
            elif self.history_cache.next_index is not None:

                # thisIndex should be the 1-6 record(s) after next_index (note: index cycles after 51199 to 0)
                indexRequested = self.history_cache.next_index
                # check if thisIndex is within the range expected
                thisIndexOk = False
                if indexRequested + 6 < KlimaLoggDriver.max_records:
                    # indexRequested 0 .. 51193
                    if indexRequested + 1 <= thisIndex <= indexRequested + 6:
                        thisIndexOk = True
                elif indexRequested == (KlimaLoggDriver.max_records - 1):
                    # indexRequested = 51199
                    if 0 <= thisIndex <= 5:
                        thisIndexOk = True
                elif thisIndex > indexRequested or thisIndex <= (indexRequested + 6 - KlimaLoggDriver.max_records):
                    # indexRequested 51194 .. 51198 and thisIndex is within one of two ranges
                    thisIndexOk = True

                if thisIndexOk:
                    # get the next 1-6 history record(s)
                    for x in range(1, 7):
                        if data.values['Pos%dAlarm' % x] == 0:
                            # History record
                            tsCurrentRec = tstr_to_ts(str(data.values['Pos%dDT' % x]))
                            # skip records which are too old or elder than requested
                            if tsCurrentRec >= self.TS_2010_07 and tsCurrentRec >= self.history_cache.since_ts:
                                # skip records with dateTime in the future
                                if tsCurrentRec > (now + 300):
                                    logdbg('handleHistoryData: skipped record at Pos%d tsCurrentRec=%s'
                                           ' DT is in the future' %
                                           (x,tsCurrentRec))
                                    self.records_skipped += 1
                                # Check if two records in a row with the same ts
                                elif tsCurrentRec == self.ts_last_rec:
                                    if DEBUG_HISTORY_DATA > 1:
                                        logdbg('handleHistoryData: skipped record at Pos%d tsCurrentRec=%s'
                                               ' DT is the same' %
                                               (x,tsCurrentRec))
                                    self.records_skipped += 1
                                # Check if this record elder than previous good record
                                elif tsCurrentRec < self.ts_last_rec:
                                    logdbg('handleHistoryData: skipped record at Pos%d tsCurrentRec=%s'
                                           ' DT is in the past' %
                                           (x,tsCurrentRec))
                                    self.records_skipped += 1
                                # Check if this record more than 7 days newer than previous good record
                                elif self.ts_last_rec != 0 and tsCurrentRec > self.ts_last_rec + 604800:
                                    logdbg('handleHistoryData: skipped record at Pos%d tsCurrentRec=%s'
                                           ' DT has too big diff' %
                                           (x,tsCurrentRec))
                                    self.records_skipped += 1
                                else:
                                    if self.history_cache.num_cached_records < self.batch_size:
                                        # append good record to the history
                                        logdbg('handleHistoryData:  append record at Pos%d tsCurrentRec=%s' %
                                               (x,tsCurrentRec))
                                        self.history_cache.records.append(data.as_dict(x))
                                        self.history_cache.num_cached_records += 1
                                        # save only TS of good records
                                        self.ts_last_rec = tsCurrentRec
                                        # save index of last appended record
                                        self.history_cache.last_this_index = thisIndex
                                    else:
                                        logdbg('handleHistoryData: record at Pos%d'
                                               ' handled in next batch' %
                                               (x))
                                        time.sleep(20)
                            # Check if this record is too old or has no date
                            elif tsCurrentRec < self.TS_2010_07:
                                logerr('handleHistoryData: skippd record at Pos%d tsCurrentRec=None DT is too old' % x)
                                self.records_skipped += 1
                            else:
                                # this record is elder than the requested start dateTime
                                logdbg('handleHistoryData: skipped record at Pos%d' %
                                       (x))
                                self.records_skipped += 1
                        self.history_cache.next_index = thisIndex
                else:
                    if nrec > 0:
                        logdbg('handleHistoryData: index mismatch: indexRequested: %s, thisIndex: %s' %
                               (indexRequested, thisIndex))
                    elif indexRequested != thisIndex:
                        logdbg('handleHistoryData: skip corrupt record: indexRequested: %s, thisIndex: %s' %
                               (indexRequested, thisIndex))
                        self.history_cache.next_index += 1
                        self.records_skipped += 1
                nextIndex = self.history_cache.next_index
            self.history_cache.num_outstanding_records = nrec
            loginf('handleHistoryData: records cached=%s, records skipped=%s, next=%s' %
                (self.history_cache.num_cached_records, self.records_skipped, nextIndex))
        self.setSleep(self.first_sleep, 0.010)
        newlen, newbuf = self.buildACKFrame(buf, ACTION_GET_HISTORY, cs, nextIndex)
        return newlen, newbuf

    def handleNextAction(self, length, buf):
        self.last_stat.update(seen_ts=int(time.time()),
                              quality=(buf[4] & 0x7f))
        cs = buf[6] | (buf[5] << 8)
        resp = buf[3]
        if resp == RESPONSE_REQ_READ_HISTORY:
            memPerc = buf[4]
            logdbg('handleNextAction: %02x (MEM percentage not read to server: %s)' % (resp, memPerc))
            self.setSleep(0.075, 0.005)
            newlen = length
            newbuf = buf
        elif resp == RESPONSE_REQ_FIRST_CONFIG:
            logdbg('handleNextAction: %02x (first-time config)' % resp)
            self.setSleep(0.075, 0.005)
            newlen, newbuf = self.buildFirstConfigFrame(cs)
        elif resp == RESPONSE_REQ_SET_CONFIG:
            logdbg('handleNextAction: %02x (set config data)' % resp)
            self.setSleep(0.075, 0.005)
            newlen, newbuf = self.buildConfigFrame(buf)
        elif resp == RESPONSE_REQ_SET_TIME:
            logdbg('handleNextAction: %02x (set time data)' % resp)
            self.setSleep(0.075, 0.005)
            newlen, newbuf = self.buildTimeFrame(buf, cs)
        else:
            logdbg('handleNextAction: %02x' % resp)
            self.setSleep(self.first_sleep, 0.010)
            newlen, newbuf = self.buildACKFrame(buf, ACTION_GET_HISTORY, cs)
        return newlen, newbuf

    def generateResponse(self, length, buf):
        if DEBUG_COMM > 1:
            logdbg('generateResponse: %s' % self.timing())
        if length == 0:
            raise BadResponse('zero length buffer')

        bufferID = (buf[0] << 8) | buf[1]
        loggerID = buf[2]
        respType = (buf[3] & 0xF0)
        if DEBUG_COMM > 1:
            logdbg("generateResponse: id=%04x resp=%x length=%x" %
                   (bufferID, respType, length))
        deviceID = self.getDeviceID()

        if bufferID == 0xF0F0 or bufferID == 0xFFFF:
            loginf('generateResponse: console not paired, attempting to pair to 0x%04x' % deviceID)
            newlen, newbuf = self.buildACKFrame(buf, ACTION_GET_CONFIG, 0xFFFF, 0xFFFF)
        elif bufferID == deviceID:
            self.set_registered_device_id(bufferID, loggerID)  # the station and transceiver are paired now
            if respType == RESPONSE_DATA_WRITTEN:
                if length == 0x07:  # 7
                    self.hid.setRX()
                    raise DataWritten()
                else:
                    raise BadResponse('len=%x resp=%x' % (length, respType))
            elif respType == RESPONSE_GET_CONFIG:
                if length == 0x7D:  # 125
                    newlen, newbuf = self.handleConfig(length, buf)
                else:
                    raise BadResponse('len=%x resp=%x' % (length, respType))
            elif respType == RESPONSE_GET_CURRENT:
                if length == 0xE5:  # 229
                    newlen, newbuf = self.handleCurrentData(length, buf)
                else:
                    raise BadResponse('len=%x resp=%x' % (length, respType))
            elif respType == RESPONSE_GET_HISTORY:
                if length == 0xB5:  # 181
                    newlen, newbuf = self.handleHistoryData(length, buf)
                else:
                    raise BadResponse('len=%x resp=%x' % (length, respType))
            elif respType == RESPONSE_REQUEST:
                if length == 0x07:  # 7
                    newlen, newbuf = self.handleNextAction(length, buf)
                    self.hid.setState(0)
                else:
                    raise BadResponse('len=%x resp=%x' % (length, respType))
            else:
                raise BadResponse('unexpected response type %x' % respType)
        else:
            if self.config_serial is None:
                logerr('generateResponse: intercepted message from device %04x with length: %02x' % (bufferID, length))
            self.setSleep(0.200, 0.005)
            raise UnknownDeviceId('unexpected device ID (id=%04x)' % bufferID)
        return newlen, newbuf

    def configureRegisterNames(self):
        self.reg_names[AX5051RegisterNames.IFMODE]     = 0x00
        self.reg_names[AX5051RegisterNames.MODULATION] = 0x41  # fsk
        self.reg_names[AX5051RegisterNames.ENCODING]   = 0x07
        self.reg_names[AX5051RegisterNames.FRAMING]    = 0x84  # 1000:0100 ##?hdlc? |1000 010 0
        self.reg_names[AX5051RegisterNames.CRCINIT3]   = 0xff
        self.reg_names[AX5051RegisterNames.CRCINIT2]   = 0xff
        self.reg_names[AX5051RegisterNames.CRCINIT1]   = 0xff
        self.reg_names[AX5051RegisterNames.CRCINIT0]   = 0xff
        self.reg_names[AX5051RegisterNames.FREQ3]      = 0x38
        self.reg_names[AX5051RegisterNames.FREQ2]      = 0x90
        self.reg_names[AX5051RegisterNames.FREQ1]      = 0x00
        self.reg_names[AX5051RegisterNames.FREQ0]      = 0x01
        self.reg_names[AX5051RegisterNames.PLLLOOP]    = 0x1d
        self.reg_names[AX5051RegisterNames.PLLRANGING] = 0x08
        self.reg_names[AX5051RegisterNames.PLLRNGCLK]  = 0x03
        self.reg_names[AX5051RegisterNames.MODMISC]    = 0x03
        self.reg_names[AX5051RegisterNames.SPAREOUT]   = 0x00
        self.reg_names[AX5051RegisterNames.TESTOBS]    = 0x00
        self.reg_names[AX5051RegisterNames.APEOVER]    = 0x00
        self.reg_names[AX5051RegisterNames.TMMUX]      = 0x00
        self.reg_names[AX5051RegisterNames.PLLVCOI]    = 0x01
        self.reg_names[AX5051RegisterNames.PLLCPEN]    = 0x01
        self.reg_names[AX5051RegisterNames.RFMISC]     = 0xb0
        self.reg_names[AX5051RegisterNames.REF]        = 0x23
        self.reg_names[AX5051RegisterNames.IFFREQHI]   = 0x20
        self.reg_names[AX5051RegisterNames.IFFREQLO]   = 0x00
        self.reg_names[AX5051RegisterNames.ADCMISC]    = 0x01
        self.reg_names[AX5051RegisterNames.AGCTARGET]  = 0x0e
        self.reg_names[AX5051RegisterNames.AGCATTACK]  = 0x11
        self.reg_names[AX5051RegisterNames.AGCDECAY]   = 0x0e
        self.reg_names[AX5051RegisterNames.CICDEC]     = 0x3f
        self.reg_names[AX5051RegisterNames.DATARATEHI] = 0x19
        self.reg_names[AX5051RegisterNames.DATARATELO] = 0x66
        self.reg_names[AX5051RegisterNames.TMGGAINHI]  = 0x01
        self.reg_names[AX5051RegisterNames.TMGGAINLO]  = 0x96
        self.reg_names[AX5051RegisterNames.PHASEGAIN]  = 0x03
        self.reg_names[AX5051RegisterNames.FREQGAIN]   = 0x04
        self.reg_names[AX5051RegisterNames.FREQGAIN2]  = 0x0a
        self.reg_names[AX5051RegisterNames.AMPLGAIN]   = 0x06
        self.reg_names[AX5051RegisterNames.AGCMANUAL]  = 0x00
        self.reg_names[AX5051RegisterNames.ADCDCLEVEL] = 0x10
        self.reg_names[AX5051RegisterNames.RXMISC]     = 0x35
        self.reg_names[AX5051RegisterNames.FSKDEV2]    = 0x00
        self.reg_names[AX5051RegisterNames.FSKDEV1]    = 0x31
        self.reg_names[AX5051RegisterNames.FSKDEV0]    = 0x27
        self.reg_names[AX5051RegisterNames.TXPWR]      = 0x03
        self.reg_names[AX5051RegisterNames.TXRATEHI]   = 0x00
        self.reg_names[AX5051RegisterNames.TXRATEMID]  = 0x51
        self.reg_names[AX5051RegisterNames.TXRATELO]   = 0xec
        self.reg_names[AX5051RegisterNames.TXDRIVER]   = 0x88

    def initTransceiver(self, frequency_standard):
        self.configureRegisterNames()

        # calculate the frequency then set frequency registers
        logdbg('frequency standard: %s' % frequency_standard)
        freq = frequencies.get(frequency_standard, frequencies['EU'])
        loginf('base frequency: %d' % freq)
        try:
            freqVal = long(freq / 16000000.0 * 16777216.0)    # python 2
        except NameError:
            freqVal = int(freq / 16000000.0 * 16777216.0)    # python 3
        corVec = self.hid.readConfigFlash(0x1F5, 4)
        corVal = corVec[0] << 8
        corVal |= corVec[1]
        corVal <<= 8
        corVal |= corVec[2]
        corVal <<= 8
        corVal |= corVec[3]
        loginf('frequency correction: %d (0x%x)' % (corVal, corVal))
        freqVal += corVal
        if not (freqVal % 2):
            freqVal += 1
        loginf('adjusted frequency: %d (0x%x)' % (freqVal, freqVal))
        self.reg_names[AX5051RegisterNames.FREQ3] = (freqVal >> 24) & 0xFF
        self.reg_names[AX5051RegisterNames.FREQ2] = (freqVal >> 16) & 0xFF
        self.reg_names[AX5051RegisterNames.FREQ1] = (freqVal >> 8)  & 0xFF
        self.reg_names[AX5051RegisterNames.FREQ0] = (freqVal >> 0)  & 0xFF
        logdbg('frequency registers: %x %x %x %x' % (
            self.reg_names[AX5051RegisterNames.FREQ3],
            self.reg_names[AX5051RegisterNames.FREQ2],
            self.reg_names[AX5051RegisterNames.FREQ1],
            self.reg_names[AX5051RegisterNames.FREQ0]))

        # figure out the transceiver id
        buf = self.hid.readConfigFlash(0x1F9, 7)
        tid = (buf[5] << 8) + buf[6]
        loginf('transceiver identifier: %d (0x%04x)' % (tid, tid))
        self.transceiver_settings.device_id = tid

        # figure out the transceiver serial number
        sn = ''.join(['%02d' % x for x in buf[0:7]])
        loginf('transceiver serial: %s' % sn)
        self.transceiver_settings.serial_number = sn

        for r in self.reg_names:
            self.hid.writeReg(r, self.reg_names[r])

    def setup(self, frequency_standard, comm_interval,
              logger_channel, vendor_id, product_id, serial):
        loginf("comm_interval is %s" % comm_interval)
        loginf("logger_channel is %s" % logger_channel)
        self.comm_mode_interval = comm_interval
        self.logger_id = logger_channel - 1
        self.config_serial = serial
        self.hid.open(vendor_id, product_id, serial)
        self.initTransceiver(frequency_standard)
        self.transceiver_present = True

    def teardown(self):
        self.transceiver_present = False
        self.hid.close()

    def getTransceiverPresent(self):
        return self.transceiver_present

    def set_registered_device_id(self, val, logger_id):
        if val != self.registered_device_id:
            loginf("console is paired to device with ID %04x and logger channel %s" % (val, logger_id + 1))
        self.registered_device_id = val

    def getDeviceRegistered(self):
        if (self.registered_device_id is None or
            self.transceiver_settings.device_id is None or
            self.registered_device_id != self.transceiver_settings.device_id):
            return False
        return True

    def getDeviceID(self):
        return self.transceiver_settings.device_id

    def getTransceiverSerNo(self):
        return self.transceiver_settings.serial_number

    # FIXME: make this thread-safe
    def getCurrentData(self):
        return self.current

    # FIXME: make this thread-safe
    def getLastStat(self):
        return self.last_stat

    # FIXME: make this thread-safe
    def getConfigData(self):
        return self.station_config

    def startCachingHistory(self, since_ts=0, num_rec=0):
        self.history_cache.clear_records()
        if since_ts is None:
            since_ts = 0
        self.history_cache.since_ts = since_ts
        if num_rec > KlimaLoggDriver.max_records - 2:
            num_rec = KlimaLoggDriver.max_records - 2
        self.history_cache.num_rec = num_rec
        self.command = ACTION_GET_HISTORY

    def stopCachingHistory(self):
        self.command = None

    def getUncachedHistoryCount(self):
        return self.history_cache.num_outstanding_records

    def getNextHistoryIndex(self):
        return self.history_cache.next_index

    def getCachedHistoryCount(self):
        return self.history_cache.num_cached_records

    def getLatestHistoryIndex(self):
        return self.last_stat.latest_history_index

    def getHistoryCacheRecords(self):
        return self.history_cache.records

    def clearHistoryCache(self):
        self.history_cache.clear_records()

    def clearWaitAtStart(self):
        self.history_cache.wait_at_start = 0

    def startRFThread(self):
        if self.child is not None:
            return
        logdbg('startRFThread: spawning RF thread')
        self.running = True
        self.child = threading.Thread(target=self.doRF)
        self.child.setName('RFComm')
        self.child.setDaemon(True)
        self.child.start()

    def stopRFThread(self):
        self.running = False
        logdbg('stopRFThread: waiting for RF thread to terminate')
        self.child.join(self.thread_wait)
        if self.child.isAlive():
            logerr('unable to terminate RF thread after %d seconds' %
                   self.thread_wait)
        else:
            self.child = None

    def isRunning(self):
        return self.running

    def doRF(self):
        try:
            logdbg('setting up rf communication')
            self.doRFSetup()
            # wait for genStartupRecords or show_current to start
            while self.history_cache.wait_at_start == 1:
                time.sleep(1)
            loginf("starting rf communication")
            while self.running:
                self.doRFCommunication()
        except Exception as e:
            logerr('exception in doRF: %s' % e)
            self.running = False
            raise
        finally:
            logdbg('stopping rf communication')

    # it is probably not necessary to have two setPreamblePattern invocations.
    # however, HeavyWeatherPro seems to do it this way on a first time config.
    # doing it this way makes configuration easier during a factory reset and
    # when re-establishing communication with the station sensors.
    def doRFSetup(self):
        self.hid.execute(5)
        self.hid.setPreamblePattern(0xaa)
        self.hid.setState(0)
        time.sleep(1)
        self.hid.setRX()

        self.hid.setPreamblePattern(0xaa)
        self.hid.setState(0x1e)
        time.sleep(1)
        self.hid.setRX()
        self.setSleep(0.075, 0.005)

    def doRFCommunication(self):
        time.sleep(self.firstSleep)
        self.pollCount = 0
        while self.running:
            statebuf = [0] * 2
            try:
                statebuf = self.hid.getState()
            except Exception as e:
                logerr('getState failed: %s' % e)
                time.sleep(5)
                pass
            self.pollCount += 1
            if statebuf[0] == 0x16:
                break
            time.sleep(self.nextSleep)
        else:
            return

        framelen, framebuf = self.hid.getFrame()
        try:
            framelen, framebuf = self.generateResponse(framelen, framebuf)
            self.hid.setFrame(framelen, framebuf)
            self.hid.setTX()
        except DataWritten:
            logdbg('SetTime/SetConfig data written')
            self.hid.setRX()
        except BadResponse as e:
            logerr('generateResponse failed: %s' % e)
            self.hid.setRX()
        except UnknownDeviceId as e:
            if self.config_serial is None:
                logerr("%s; use parameter 'serial' if more than one USB transceiver present" % e)
            self.hid.setRX()

    # these are for diagnostics and debugging
    def setSleep(self, firstsleep, nextsleep):
        self.firstSleep = firstsleep
        self.nextSleep = nextsleep

    def timing(self):
        s = self.firstSleep + self.nextSleep * (self.pollCount - 1)
        return 'sleep=%s first=%s next=%s count=%s' % (
            s, self.firstSleep, self.nextSleep, self.pollCount)







class HistoryData(object):

    BUFMAPHIS = {1: (176,
                     (174,173,171,170,168,167,165,164,162),
                     (161,160,159,158,157,156,155,154,153)),
                 2: (148,
                     (146,145,143,142,140,139,137,136,134),
                     (133,132,131,130,129,128,127,126,125)),
                 3: (120,
                     (118,117,115,114,112,111,109,108,106),
                     (105,104,103,102,101,100, 99, 98, 97)),
                 4: ( 92,
                     ( 90, 89, 87, 86, 84, 83, 81, 80, 78),
                     ( 77, 76, 75, 74, 73, 72, 71, 70, 69)),
                 5: ( 64,
                     ( 62, 61, 59, 58, 56, 55, 53, 52, 50),
                     ( 49, 48, 47, 46, 45, 44, 43, 42, 41)),
                 6: ( 36,
                     ( 34, 33, 31, 30, 28, 27, 25, 24, 22),
                     ( 21, 20, 19, 18, 17, 16, 15, 14, 13))}

    BUFMAPALA = {1: (180,175,174,172,170,169,168,167,166),
                 2: (152,147,146,144,142,141,140,139,138),
                 3: (124,119,118,116,114,113,112,111,110),
                 4: ( 96, 91, 90, 88, 86, 85, 84, 83, 82),
                 5: ( 68, 63, 62, 60, 58, 57, 56, 55, 54),
                 6: ( 40, 35, 34, 32, 30, 29, 28, 27, 26)}

    def __init__(self):
        self.values = {}
        for i in range(1, 7):
            self.values['Pos%dAlarm' % i] = 0
            self.values['Pos%dDT' % i] = datetime(1900, 1, 1, 0, 0)
            self.values['Pos%dHumidityHi' % i] = SensorLimits.humidity_NP
            self.values['Pos%dHumidityLo' % i] = SensorLimits.humidity_NP
            self.values['Pos%dHumidity' % i] = SensorLimits.humidity_NP
            self.values['Pos%dTempHi' % i] = SensorLimits.temperature_NP
            self.values['Pos%dTempLo' % i] = SensorLimits.temperature_NP
            self.values['Pos%dTemp' % i] = SensorLimits.temperature_NP
            self.values['Pos%dAlarmdata' % i] = 0
            self.values['Pos%dSensor' % i] = 0
            for j in range(0, 9):
                self.values['Pos%dTemp%d' % (i, j)] = SensorLimits.temperature_NP
                self.values['Pos%dHumidity%d' % (i, j)] = SensorLimits.humidity_NP

    def read(self, buf):
        values = {}
        for i in range(1, 7):
            values['Pos%dAlarm' % i] = 1 if buf[self.BUFMAPALA[i][0]] == 0xee else 0
            if values['Pos%dAlarm' % i] == 0:
                # History record
                values['Pos%dDT' % i] = Decode.toDateTime10(
                    buf, self.BUFMAPHIS[i][0], 1, 'HistoryData%d' % i)
                for j in range(0, 9):
                    values['Pos%dTemp%d' % (i, j)] = Decode.toTemperature_3_1(
                        buf, self.BUFMAPHIS[i][1][j], j % 2)
                    values['Pos%dHumidity%d' % (i, j)] = Decode.toHumidity_2_0(
                        buf, self.BUFMAPHIS[i][2][j], 1)
            else:
                # Alarm record
                values['Pos%dDT' % i] = Decode.toDateTime10(
                    buf, self.BUFMAPALA[i][1], 1, 'HistoryData%d' % i)
                values['Pos%dHumidityHi' % i] = Decode.toHumidity_2_0(
                    buf, self.BUFMAPALA[i][8], 1)
                values['Pos%dHumidityLo' % i] = Decode.toHumidity_2_0(
                    buf, self.BUFMAPALA[i][7], 1)
                values['Pos%dHumidity' % i] = Decode.toHumidity_2_0(
                    buf, self.BUFMAPALA[i][6], 1)
                values['Pos%dTempHi' % i] = Decode.toTemperature_3_1(
                    buf, self.BUFMAPALA[i][5], 1)
                values['Pos%dTempLo' % i] = Decode.toTemperature_3_1(
                    buf, self.BUFMAPALA[i][4], 0)
                values['Pos%dTemp' % i] = Decode.toTemperature_3_1(
                    buf, self.BUFMAPALA[i][3], 0)
                values['Pos%dAlarmdata' % i] = (buf[self.BUFMAPALA[i][2]] >> 4) & 0xf
                values['Pos%dSensor' % i] = buf[self.BUFMAPALA[i][2]] & 0xf
        self.values = values

    def to_log(self):
        last_ts = None
        for i in range(1, 7):
            if self.values['Pos%dAlarm' % i] == 0:
                # History record
                if self.values['Pos%dDT' % i] != last_ts:
                    logdbg("Pos%dDT %s, Pos%dTemp0: %3.1f, Pos%sHumidity0: %3.1f" %
                           (i, self.values['Pos%dDT' % i],
                            i, self.values['Pos%dTemp0' % i],
                            i, self.values['Pos%dHumidity0' % i]))
                    logdbg("Pos%dTemp 1-8:      %3.1f, %3.1f, %3.1f, %3.1f, %3.1f, %3.1f, %3.1f, %3.1f" %
                           (i,
                            self.values['Pos%dTemp1' % i],
                            self.values['Pos%dTemp2' % i],
                            self.values['Pos%dTemp3' % i],
                            self.values['Pos%dTemp4' % i],
                            self.values['Pos%dTemp5' % i],
                            self.values['Pos%dTemp6' % i],
                            self.values['Pos%dTemp7' % i],
                            self.values['Pos%dTemp8' % i]))
                    logdbg("Pos%dHumidity 1-8: %3.0f, %3.0f, %3.0f, %3.0f, %3.0f, %3.0f, %3.0f, %3.0f" %
                           (i,
                            self.values['Pos%dHumidity1' % i],
                            self.values['Pos%dHumidity2' % i],
                            self.values['Pos%dHumidity3' % i],
                            self.values['Pos%dHumidity4' % i],
                            self.values['Pos%dHumidity5' % i],
                            self.values['Pos%dHumidity6' % i],
                            self.values['Pos%dHumidity7' % i],
                            self.values['Pos%dHumidity8' % i]))
                last_ts = self.values['Pos%dDT' % i]
            else:
                # Alarm record
                if self.values['Pos%dAlarmdata' % i] & 0x1:
                    logdbg('Alarm=%01x: Humidity%d: %3.0f above/reached Hi-limit (%3.0f) on %s' %
                           (self.values['Pos%dAlarmdata' % i],
                            self.values['Pos%dSensor' % i],
                            self.values['Pos%dHumidity' % i],
                            self.values['Pos%dHumidityHi' % i],
                            self.values['Pos%dDT' % i]))
                if self.values['Pos%dAlarmdata' % i] & 0x2:
                    logdbg('Alarm=%01x: Humidity%d: %3.0f below/reached Lo-limit (%3.0f) on %s' %
                           (self.values['Pos%dAlarmdata' % i],
                            self.values['Pos%dSensor' % i],
                            self.values['Pos%dHumidity' % i],
                            self.values['Pos%dHumidityLo' % i],
                            self.values['Pos%dDT' % i]))
                if self.values['Pos%dAlarmdata' % i] & 0x4:
                    logdbg('Alarm=%01x: Temp%d: %3.1f above/reached Hi-limit (%3.1f) on %s' %
                           (self.values['Pos%dAlarmdata' % i],
                            self.values['Pos%dSensor' % i],
                            self.values['Pos%dTemp' % i],
                            self.values['Pos%dTempHi' % i],
                            self.values['Pos%dDT' % i]))
                if self.values['Pos%dAlarmdata' % i] & 0x8:
                    logdbg('Alarm=%01x: Temp%d: %3.1f below/reached Lo-limit(%3.1f) on %s' %
                           (self.values['Pos%dAlarmdata' % i],
                            self.values['Pos%dSensor' % i],
                            self.values['Pos%dTemp' % i],
                            self.values['Pos%dTempLo' % i],
                            self.values['Pos%dDT' % i]))

    def as_dict(self, x=1):
        """emit historical data as a dict with weewx conventions"""
        data = {'dateTime': tstr_to_ts(str(self.values['Pos%dDT' % x]))}
        for y in range(0, 9):
            data['Temp%d' % y] = self.values['Pos%dTemp%d' % (x, y)]
            data['Humidity%d' % y] = self.values['Pos%dHumidity%d' % (x, y)]
        return data










class HistoryCache:
    def __init__(self):
        self.wait_at_start = 1
        self.clear_records()

    def clear_records(self):
        self.since_ts = 0
        self.num_rec = 0
        self.start_index = None
        self.next_index = None
        self.records = []
        self.num_outstanding_records = None
        self.num_cached_records = 0
        self.last_ts = 0









class Decode(object):

    CHARMAP = (' ', '1', '2', '3', '4', '5', '6', '7', '8', '9',
               '0', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
               'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
               'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '-', '+', '(',
               ')', 'o', '*', ',', '/', '\\', ' ', '.', ' ', ' ',
               ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
               ' ', ' ', ' ', '@')

    CHARSTR = "!1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ-+()o*,/\ ."

    @staticmethod
    def toCharacters3_2(buf, start, startOnHiNibble):
        """read 3 (4 bits) nibbles, presentation as 2 (6 bit) characters"""
        if startOnHiNibble:
            idx1 = ((buf[start + 1] >> 2) & 0x3C) + ((buf[start] >> 2) & 0x3)
            idx2 = ((buf[start] << 4) & 0x30) + ((buf[start] >> 4) & 0xF)
        else:
            idx1 = ((buf[start + 1] << 2) & 0x3C) + ((buf[start + 1] >> 6) & 0x3)
            idx2 = (buf[start + 1] & 0x30) + (buf[start] & 0xF)
        return Decode.CHARMAP[idx1] + Decode.CHARMAP[idx2]

    @staticmethod
    def isOFL2(buf, start, startOnHiNibble):
        if startOnHiNibble:
            result = ((buf[start + 0] >>  4) == 15 or
                      (buf[start + 0] & 0xF) == 15)
        else:
            result = ((buf[start + 0] & 0xF) == 15 or
                      (buf[start + 1] >>  4) == 15)
        return result

    @staticmethod
    def isOFL3(buf, start, startOnHiNibble):
        if startOnHiNibble:
            result = ((buf[start + 0] >>  4) == 15 or
                      (buf[start + 0] & 0xF) == 15 or
                      (buf[start + 1] >>  4) == 15)
        else:
            result = ((buf[start + 0] & 0xF) == 15 or
                      (buf[start + 1] >>  4) == 15 or
                      (buf[start + 1] & 0xF) == 15)
        return result

    @staticmethod
    def isOFL5(buf, start, startOnHiNibble):
        if startOnHiNibble:
            result = ((buf[start + 0] >>  4) == 15 or
                      (buf[start + 0] & 0xF) == 15 or
                      (buf[start + 1] >>  4) == 15 or
                      (buf[start + 1] & 0xF) == 15 or
                      (buf[start + 2] >>  4) == 15)
        else:
            result = ((buf[start + 0] & 0xF) == 15 or
                      (buf[start + 1] >>  4) == 15 or
                      (buf[start + 1] & 0xF) == 15 or
                      (buf[start + 2] >>  4) == 15 or
                      (buf[start + 2] & 0xF) == 15)
        return result

    @staticmethod
    def isErr2(buf, start, startOnHiNibble):
        if startOnHiNibble:
            result = ((buf[start + 0] >>  4) >= 10 and
                      (buf[start + 0] >>  4) != 15 or
                      (buf[start + 0] & 0xF) >= 10 and
                      (buf[start + 0] & 0xF) != 15)
        else:
            result = ((buf[start + 0] & 0xF) >= 10 and
                      (buf[start + 0] & 0xF) != 15 or
                      (buf[start + 1] >>  4) >= 10 and
                      (buf[start + 1] >>  4) != 15)
        return result
        
    @staticmethod
    def isErr3(buf, start, startOnHiNibble):
        if startOnHiNibble:
            result = ((buf[start + 0] >>  4) >= 10 and
                      (buf[start + 0] >>  4) != 15 or
                      (buf[start + 0] & 0xF) >= 10 and
                      (buf[start + 0] & 0xF) != 15 or
                      (buf[start + 1] >>  4) >= 10 and
                      (buf[start + 1] >>  4) != 15)
        else:
            result = ((buf[start + 0] & 0xF) >= 10 and
                      (buf[start + 0] & 0xF) != 15 or
                      (buf[start + 1] >>  4) >= 10 and
                      (buf[start + 1] >>  4) != 15 or
                      (buf[start + 1] & 0xF) >= 10 and
                      (buf[start + 1] & 0xF) != 15)
        return result
        
    @staticmethod
    def isErr5(buf, start, startOnHiNibble):
        if startOnHiNibble:
            result = ((buf[start + 0] >>  4) >= 10 and
                      (buf[start + 0] >>  4) != 15 or
                      (buf[start + 0] & 0xF) >= 10 and
                      (buf[start + 0] & 0xF) != 15 or
                      (buf[start + 1] >>  4) >= 10 and
                      (buf[start + 1] >>  4) != 15 or
                      (buf[start + 1] & 0xF) >= 10 and
                      (buf[start + 1] & 0xF) != 15 or
                      (buf[start + 2] >>  4) >= 10 and
                      (buf[start + 2] >>  4) != 15)
        else:
            result = ((buf[start + 0] & 0xF) >= 10 and
                      (buf[start + 0] & 0xF) != 15 or
                      (buf[start + 1] >>  4) >= 10 and
                      (buf[start + 1] >>  4) != 15 or
                      (buf[start + 1] & 0xF) >= 10 and
                      (buf[start + 1] & 0xF) != 15 or
                      (buf[start + 2] >>  4) >= 10 and
                      (buf[start + 2] >>  4) != 15 or
                      (buf[start + 2] & 0xF) >= 10 and
                      (buf[start + 2] & 0xF) != 15)
        return result

    @staticmethod
    def isErr8(buf, start, startOnHiNibble):
        if startOnHiNibble:
            result = ((buf[start + 0] >>  4) == 10 and
                      (buf[start + 0] & 0xF) == 10 and
                      (buf[start + 1] >>  4) == 4  and
                      (buf[start + 1] & 0xF) == 10 and
                      (buf[start + 2] >>  4) == 10 and
                      (buf[start + 2] & 0xF) == 4  and
                      (buf[start + 3] >>  4) == 10 and
                      (buf[start + 3] & 0xF) == 10)
        else:
            result = ((buf[start + 0] & 0xF) == 10 and
                      (buf[start + 1] >>  4) == 10 and
                      (buf[start + 1] & 0xF) == 4  and
                      (buf[start + 2] >>  4) == 10 and
                      (buf[start + 2] & 0xF) == 10 and
                      (buf[start + 3] >>  4) == 4  and
                      (buf[start + 3] & 0xF) == 10 and
                      (buf[start + 4] >>  4) == 10)
        return result

    @staticmethod
    def toInt_1(buf, start, startOnHiNibble):
        """read 1 nibble"""
        if startOnHiNibble:
            rawpre = (buf[start] >> 4)
        else:
            rawpre = (buf[start] & 0xF)
        return rawpre

    @staticmethod
    def toInt_2(buf, start, startOnHiNibble):
        """read 2 nibbles"""
        if startOnHiNibble:
            rawpre = (buf[start] >> 4) * 10 + (buf[start + 0] & 0xF) * 1
        else:
            rawpre = (buf[start] & 0xF) * 10 + (buf[start + 1] >> 4) * 1
        return rawpre

    @staticmethod
    def toDateTime10(buf, start, startOnHiNibble, label):
        """read 10 nibbles, presentation as DateTime"""
        result = None
        if (Decode.isErr2(buf, start + 0, startOnHiNibble) or
            Decode.isErr2(buf, start + 1, startOnHiNibble) or
            Decode.isErr2(buf, start + 2, startOnHiNibble) or
            Decode.isErr2(buf, start + 3, startOnHiNibble) or
            Decode.isErr2(buf, start + 4, startOnHiNibble)):
            logerr('ToDateTime: bogus date for %s: error status in buffer' %
                   label)
        else:
            year    = Decode.toInt_2(buf, start + 0, startOnHiNibble) + 2000
            month   = Decode.toInt_2(buf, start + 1, startOnHiNibble)
            days    = Decode.toInt_2(buf, start + 2, startOnHiNibble)
            hours   = Decode.toInt_2(buf, start + 3, startOnHiNibble)
            minutes = Decode.toInt_2(buf, start + 4, startOnHiNibble)
            try:
                result = datetime(year, month, days, hours, minutes)
            except ValueError:
                logerr(('ToDateTime: bogus date for %s:'
                        ' bad date conversion from'
                        ' %s %s %s %s %s') %
                       (label, minutes, hours, days, month, year))
        if result is None:
            # FIXME: use None instead of a really old date to indicate invalid
            result = datetime(1900, 1, 1, 0, 0)
        return result

    @staticmethod
    def toDateTime8(buf, start, startOnHiNibble, label):
        """read 8 nibbles, presentation as DateTime"""
        result = None
        if Decode.isErr8(buf, start + 0, startOnHiNibble):
            logerr('ToDateTime: %s: no valid date' % label)
        else:
            if startOnHiNibble:
                year  = Decode.toInt_2(buf, start + 0, 1) + 2000
                month = Decode.toInt_1(buf, start + 1, 1)
                days  = Decode.toInt_2(buf, start + 1, 0)
                tim1  = Decode.toInt_1(buf, start + 2, 0)
                tim2  = Decode.toInt_1(buf, start + 3, 1)
                tim3  = Decode.toInt_1(buf, start + 3, 0)
            else:
                year  = Decode.toInt_2(buf, start + 0, 0) + 2000
                month = Decode.toInt_1(buf, start + 1, 0)
                days  = Decode.toInt_2(buf, start + 2, 1)
                tim1  = Decode.toInt_1(buf, start + 3, 1)
                tim2  = Decode.toInt_1(buf, start + 3, 0)
                tim3  = Decode.toInt_1(buf, start + 4, 1)
            if tim1 >= 10:
                hours = tim1 + 10
            else:
                hours = tim1
            if tim2 >= 10:
                hours += 10
                minutes = (tim2 - 10) * 10
            else:
                minutes = tim2 * 10
            minutes += tim3
            try:
                result = datetime(year, month, days, hours, minutes)
            except ValueError:
                logerr('ToDateTime: bogus date for %s:'
                       ' bad date conversion from'
                       ' %s %s %s %s %s' %
                       (label, minutes, hours, days, month, year))
        if result is None:
            # FIXME: use None instead of a really old date to indicate invalid
            result = datetime(1900, 1, 1, 0, 0)
        return result

    @staticmethod
    def toHumidity_2_0(buf, start, startOnHiNibble):
        """read 2 nibbles, presentation with 0 decimal"""
        if Decode.isErr2(buf, start, startOnHiNibble):
            result = SensorLimits.humidity_NP
        elif Decode.isOFL2(buf, start, startOnHiNibble):
            result = SensorLimits.humidity_OFL
        else:
            result = Decode.toInt_2(buf, start, startOnHiNibble)
        return result

    @staticmethod
    def toTemperature_3_1(buf, start, startOnHiNibble):
        """read 3 nibbles, presentation with 1 decimal; units of degree C"""
        if Decode.isErr3(buf, start, startOnHiNibble):
            result = SensorLimits.temperature_NP
        elif Decode.isOFL3(buf, start, startOnHiNibble):
            result = SensorLimits.temperature_OFL
        else:
            if startOnHiNibble:
                rawtemp = (buf[start] >> 4) * 10 \
                    + (buf[start + 0] & 0xF) * 1 \
                    + (buf[start + 1] >> 4) * 0.1
            else:
                rawtemp = (buf[start] & 0xF) * 10 \
                    + (buf[start + 1] >> 4) * 1 \
                    + (buf[start + 1] & 0xF) * 0.1
            result = rawtemp - SensorLimits.temperature_offset
        return result







class KlimaLoggDriver():
    """Driver for TFA KlimaLogg stations."""

    # maximum number of history records
    # record number range: 0-51199
    # address range: 0x070000-0x1fffe0
    max_records = 51200

    def __init__(self):
        """Initialize the station object.

        model: Which station model is this?
        [Optional. Default is 'TFA KlimaLogg Pro']

        transceiver_frequency: Frequency for transceiver-to-console.  Specify
        either US or EU.
        [Required. Default is EU]

        polling_interval: How often to sample the USB interface for data.
        [Optional. Default is 10 seconds]

        comm_interval: Communications mode interval
        [Optional.  Default is 8]

        serial: The transceiver serial number.  If there are multiple
        devices with the same vendor and product IDs on the bus, each will
        have a unique serial number.  Use the serial number to indicate which
        transceiver should be used.
        [Optional.  Default is None]

        logger_channel: Use the logger channel to identify the console.
        [Optional.  Default is 1]

        sensor_map_id: Either 0 (mapping for kl schema) or 1 (for wview schema)
        Used only if no sensor map is specified.
        [Optional.  Default is 0]

        sensor_map: Indicates the mapping between sensor name and field in the
        database schema.  Only observations in this map will be included in
        LOOP packets.  If no sensor map is specified, then the sensor_map_id
        is used to pick a mapping.
        [Optional.  Default is None]

        max_history_records: Maximum number of historical records to read from
        the logger.
        [Optional.  Default is 51200]

        batch_size: Number of records to read in each tranche while reading
        records from the logger.
        [Optional.  Default is 1800]
        """
        loginf('driver version is %s' % DRIVER_VERSION)
        self.vendor_id = stn_dict.get('vendor_id', 0x6666)
        self.product_id = stn_dict.get('product_id', 0x5555)
        self.model = stn_dict.get('model', 'TFA KlimaLogg Pro')
        self.polling_interval = int(stn_dict.get('polling_interval', 10))
        self.comm_interval = int(stn_dict.get('comm_interval', 8))
        self.logger_channel = int(stn_dict.get('logger_channel', 1))
        loginf('channel is %s' % self.logger_channel)
        self.frequency = stn_dict.get('transceiver_frequency', 'EU')
        loginf('frequency is %s' % self.frequency)
        self.config_serial = stn_dict.get('serial', None)
        if self.config_serial is not None:
            loginf('serial is %s' % self.config_serial)
        self.sensor_map = stn_dict.get('sensor_map', None)
        if self.sensor_map is None:
            sensor_map_id = int(stn_dict.get('sensor_map_id', 0))
            self.sensor_map = KL_SENSOR_MAP
            logdbg('using sensor map for kl schema')
        else:
            logdbg('using custom sensor map')
        loginf('sensor map is: %s' % self.sensor_map)
        self.max_history_records = int(stn_dict.get('max_history_records', 51200))
        loginf('catchup limited to %s records' % self.max_history_records)
        self.batch_size = int(stn_dict.get('batch_size', 1800))
        timing = int(stn_dict.get('timing', 300))
        self.first_sleep = float(timing) / 1000.0
        loginf('timing is %s ms (%0.3f s)' % (timing, self.first_sleep))
        self.values = dict()
        for i in range(1, 9):
            self.values['sensor_text%d' % i] = stn_dict.get('sensor_text%d' % i, None)

        now = int(time.time())
        self._service = None
        self._last_obs_ts = None
        self._last_nodata_log_ts = now
        self._nodata_interval = 300  # how often to check for no data
        self._last_contact_log_ts = now
        self._nocontact_interval = 300  # how often to check for no contact
        self._log_interval = 600  # how often to log
        self._packet_count = 0
        self._empty_packet_count = 0


        self.startUp()

    def show_history(self, maxtries, ts=0, count=0):
        """Display the indicated number of records or the records since the 
        specified timestamp (local time, in seconds)"""
        print("Querying the station for historical records...")
        ntries = 0
        last_n = nrem = None
        last_ts = int(time.time())
        self.start_caching_history(since_ts=ts, num_rec=count)
        while nrem is None or nrem > 0:
            if ntries >= maxtries:
                print('Giving up after %d tries' % ntries)
                break
            time.sleep(30)
            ntries += 1
            now = int(time.time())
            n = self.get_next_history_index()
            if n == last_n:
                dur = now - last_ts
                print('No data after %d seconds (%s)' % (dur, PRESS_USB))
            else:
                ntries = 0
                last_ts = now
            last_n = n
            nrem = self.get_uncached_history_count()
            ni = self.get_next_history_index()
            li = self.get_latest_history_index()
            print("  Scanned %s record sets: current=%s latest=%s remaining=%s\r" % (n, ni, li, nrem))
            sys.stdout.flush()
        self.stop_caching_history()
        records = self.get_history_cache_records()
        self.clear_history_cache()
        print('Found %d records' % len(records))
        for r in records:
            print(r)

    @property
    def hardware_name(self):
        return self.model

    # this is invoked by StdEngine as it shuts down
    def closePort(self):
        self.shutDown()

    def genLoopPackets(self):
        """Generator function that continuously returns decoded packets."""
        while True:
            self._packet_count += 1
            now = int(time.time() + 0.5)
            packet = self.get_observation()
            if packet is not None:
                ts = packet['dateTime']
                if DEBUG_WEATHER_DATA > 0:
                    logdbg('genLoopPackets: packet_count=%s: ts=%s packet=%s' %
                           (self._packet_count, ts, packet))
                if self._last_obs_ts is None or self._last_obs_ts != ts:
                    self._last_obs_ts = ts
                    self._empty_packet_count = 0
                    self._last_nodata_log_ts = now
                    self._last_contact_log_ts = now
                else:
                    self._empty_packet_count += 1
                    if DEBUG_WEATHER_DATA > 0 and self._empty_packet_count > 1:
                        logdbg("genLoopPackets: timestamp unchanged, set to empty packet; count: %s" %
                               self._empty_packet_count)
                    packet = None
            else:
                self._empty_packet_count += 1
                if DEBUG_WEATHER_DATA > 0 and self._empty_packet_count > 1:
                    logdbg("genLoopPackets: empty packet; count; %s" % self._empty_packet_count)

            # if no new weather data, return an empty packet
            if packet is None:
                if DEBUG_WEATHER_DATA > 0:
                    logdbg("packet_count=%s empty_count=%s" %
                           (self._packet_count, self._empty_packet_count))
                if self._empty_packet_count >= 30:  # 30 * 10 s = 300 s
                    if DEBUG_WEATHER_DATA > 0:
                        msg = "Restarting communication after %d empty packets" % self._empty_packet_count
                        logdbg(msg)
                        raise NameError('%s (%s)' % (msg, PRESS_USB))
                packet = {'usUnits': 0, 'dateTime': now}
                # if no new weather data for awhile, log it
                if (self._last_obs_ts is None or
                    now - self._last_obs_ts > self._nodata_interval):
                    if now - self._last_nodata_log_ts > self._log_interval:
                        msg = 'no new weather data'
                        if self._last_obs_ts is not None:
                            msg += ' after %d seconds' % (
                                now - self._last_obs_ts)
                        loginf(msg)
                        self._last_nodata_log_ts = now

            # if no contact with console for awhile, log it
            ts = self.get_last_contact()
            if ts is None or now - ts > self._nocontact_interval:
                if now - self._last_contact_log_ts > self._log_interval:
                    msg = 'no contact with console'
                    if ts is not None:
                        msg += ' after %d seconds' % (now - ts)
                    msg += ' (%s)' % PRESS_USB
                    loginf(msg)
                    self._last_contact_log_ts = now

            yield packet
            time.sleep(self.polling_interval)                    

    def genStartupRecords(self, ts):
        loginf('Scanning historical records')
        self.clear_wait_at_start()  # let rf communication start
        first_ts = ts
        max_store_period = 300  # do another batch when period to save records is more than max_store_period
        batch_started = False
        records_handled = 0
        n = 0
        store_period = max_store_period
        while store_period >= max_store_period:
            maxtries = 1445  # once per day at 00:00 the communication starts automatically ???
            ntries = 0
            last_n = nrem = None
            last_ts = int(time.time())
            self.start_caching_history(since_ts=first_ts)
            while nrem is None or nrem > 0:
                if ntries >= maxtries:
                    logerr('No historical data after %d tries' % ntries)
                    return
                time.sleep(15)
                ntries += 1
                now = int(time.time())
                n = self.get_cached_history_count()
                batch_started = n > 0
                if n == last_n:
                    dur = now - last_ts
                    if not batch_started:
                        if records_handled == 0:
                            logtee(PRESS_USB)
                        else:
                            logtee("The next batch of %s records will start within 5 minutes" % self.batch_size)
                else:
                    ntries = 0
                    last_ts = now
                last_n = n
                nrem = self.get_uncached_history_count()
                ni = self.get_next_history_index()
                li = self.get_latest_history_index()
                if batch_started and n != 0:
                    logtee("Records scanned: %s" % n)
                # handle historical records in batches of batch_size
                if n >= self.batch_size:
                    break
            self.stop_caching_history()
            records = self.get_history_cache_records()
            self.clear_history_cache()
            num_received = len(records) - 1
            logtee('Found %d historical records' % num_received)
            last_ts = None
            this_ts = None
            for r in records:
                this_ts = r['dateTime']
                records_handled += 1
                logtee("Handle record {}: {}".format(records_handled, this_ts))
                if last_ts is not None:
                    rec = dict()
                    rec['usUnits'] = 0
                    rec['dateTime'] = this_ts
                    rec['interval'] = (this_ts - last_ts) / 60

                    # get values requested from the sensor map
                    for k in self.sensor_map:
                        label = self.sensor_map[k]
                        if label in r:
                            if label.startswith('Temp'):
                                x = get_datum_diff(r[label],
                                                   SensorLimits.temperature_NP,
                                                   SensorLimits.temperature_OFL)
                            elif label.startswith('Humidity'):
                                x = get_datum_diff(r[label],
                                                   SensorLimits.humidity_NP,
                                                   SensorLimits.humidity_OFL)
                            else:
                                x = r[label]
                            rec[k] = x
                    yield rec
                last_ts = this_ts
            # go for another scan when store_period is greater than
            # max_store_period
            if this_ts is not None:
                store_period = int(time.time()) - this_ts
                logtee("Saved {} historical records; ts last saved record {}".format(
                       num_received, this_ts))
                if n >= self.batch_size:
                    first_ts = this_ts  # continue next batch with last found time stamp
                    logtee('Scan the next batch of %d historical records' % self.batch_size)
                    logtee("The scan will start after the next historical record is received.")
                elif store_period >= max_store_period:
                    first_ts = this_ts  # continue next batch with last found time stamp
                    logtee('Scan the historical records missed during the store period of %d s' % store_period)
                    logtee("The scan will start after the next historical record is received.")
            else:
                store_period = 0

    def startUp(self):
        if self._service is not None:
            return
        self._service = CommunicationService(self.first_sleep, self.values,
                                             self.max_history_records,
                                             self.batch_size)
        self._service.setup(self.frequency, self.comm_interval,
                            self.logger_channel, self.vendor_id,
                            self.product_id, self.config_serial)
        self._service.startRFThread()

    def shutDown(self):
        self._service.stopRFThread()
        self._service.teardown()
        self._service = None

    def transceiver_is_present(self):
        return self._service.getTransceiverPresent()

    def transceiver_is_paired(self):
        return self._service.getDeviceRegistered()

    def get_transceiver_serial(self):
        return self._service.getTransceiverSerNo()

    def get_transceiver_id(self):
        return self._service.getDeviceID()

    def get_last_contact(self):
        return self._service.getLastStat().last_seen_ts

    SENSOR_KEYS = ['Temp0', 'Humidity0',
                   'Temp1', 'Humidity1',
                   'Temp2', 'Humidity2',
                   'Temp3', 'Humidity3',
                   'Temp4', 'Humidity4',
                   'Temp5', 'Humidity5',
                   'Temp6', 'Humidity6',
                   'Temp7', 'Humidity7',
                   'Temp8', 'Humidity8']

    def get_observation(self):
        data = self._service.getCurrentData()
        ts = data.values['timestamp']
        if ts is None:
            return None

        # MS: without weex no usUnits...? 
        # instead of packet = {'usUnits': weewx.METRIC, 'dateTime': ts} i'll use
        # usUnits=0, also on other occurences of usUnits
        packet = {'usUnits': 0, 'dateTime': ts}

        # extract the values from the data object
        for k in self.sensor_map:
            label = self.sensor_map[k]
            if label.startswith('BatteryStatus') and 'AlarmData' in data.values:
                if label == 'BatteryStatus0':
                    x = 1 if data.values['AlarmData'][1] ^ 0x80 == 0 else 0
                else:
                    n = int(label[-1]) - 1
                    bitmask = 1 << n
                    x = 1 if data.values['AlarmData'][0] ^ bitmask == 0 else 0
                packet[k] = x
            elif label in data.values:
                if label.startswith('Temp'):
                    x = get_datum_diff(data.values[label],
                                       SensorLimits.temperature_NP,
                                       SensorLimits.temperature_OFL)
                elif label.startswith('Humidity'):
                    x = get_datum_diff(data.values[label],
                                       SensorLimits.humidity_NP,
                                       SensorLimits.humidity_OFL)
                else:
                    x = data.values[label]
                packet[k] = x

        return packet

    def get_config(self):
        logdbg('get station configuration')
        cfg = self._service.getConfigData().as_dict()
        cs = cfg.get('checksum_out')
        if cs is None or cs == 0:
            return None
        return cfg

    def start_caching_history(self, since_ts=0, num_rec=0):
        self._service.startCachingHistory(since_ts, num_rec)

    def stop_caching_history(self):
        self._service.stopCachingHistory()

    def get_uncached_history_count(self):
        return self._service.getUncachedHistoryCount()

    def get_next_history_index(self):
        return self._service.getNextHistoryIndex()

    def get_latest_history_index(self):
        return self._service.getLatestHistoryIndex()

    def get_cached_history_count(self):
        return self._service.getCachedHistoryCount()

    def get_history_cache_records(self):
        return self._service.getHistoryCacheRecords()

    def clear_history_cache(self):
        self._service.clearHistoryCache()

    def clear_wait_at_start(self):
        self._service.clearWaitAtStart()








class CurrentData(object):

    BUFMAP = {0: ( 26, 28, 29, 18, 22, 15, 16, 17,  7, 11),
              1: ( 50, 52, 53, 42, 46, 39, 40, 41, 31, 35),
              2: ( 74, 76, 77, 66, 70, 63, 64, 65, 55, 59),
              3: ( 98,100,101, 90, 94, 87, 88, 89, 79, 83),
              4: (122,124,125,114,118,111,112,113,103,107),
              5: (146,148,149,138,142,135,136,137,127,131),
              6: (170,172,173,162,166,159,160,161,151,155),
              7: (194,196,197,186,190,183,184,185,175,179),
              8: (218,220,221,210,214,207,208,209,199,203)}

    def __init__(self):
        self.values = dict()
        self.values['timestamp'] = None
        self.values['SignalQuality'] = None
        for i in range(0, 9):
            self.values['Temp%d' % i] = SensorLimits.temperature_NP
            self.values['Temp%dMax' % i] = SensorLimits.temperature_NP
            self.values['Temp%dMaxDT'] = None
            self.values['Temp%dMin' % i] = SensorLimits.temperature_NP
            self.values['Temp%dMinDT'] = None
            self.values['Humidity%d' % i] = SensorLimits.humidity_NP
            self.values['Humidity%dMax' % i] = SensorLimits.humidity_NP
            self.values['Humidity%dMaxDT'] = None
            self.values['Humidity%dMin' % i] = SensorLimits.humidity_NP
            self.values['Humidity%dMinDT'] = None

    def read(self, buf):
        values = dict()
        values['timestamp'] = int(time.time() + 0.5)
        values['SignalQuality'] = buf[4] & 0x7F
        for x in range(0, 9):
            lbl = 'Temp%s' % x
            values[lbl + 'Max'] = Decode.toTemperature_3_1(buf, self.BUFMAP[x][0], 0)
            values[lbl + 'Min'] = Decode.toTemperature_3_1(buf, self.BUFMAP[x][1], 1)
            values[lbl] = Decode.toTemperature_3_1(buf, self.BUFMAP[x][2], 0)
            values[lbl + 'MaxDT'] = None if values[lbl + 'Max'] == SensorLimits.temperature_NP or values[lbl + 'Max'] == SensorLimits.temperature_OFL else Decode.toDateTime8(buf, self.BUFMAP[x][3], 0, lbl + 'Max')
            values[lbl + 'MinDT'] = None if values[lbl + 'Min'] == SensorLimits.temperature_NP or values[lbl + 'Min'] == SensorLimits.temperature_OFL else Decode.toDateTime8(buf, self.BUFMAP[x][4], 0, lbl + 'Min')
            lbl = 'Humidity%s' % x
            values[lbl + 'Max'] = Decode.toHumidity_2_0(buf, self.BUFMAP[x][5], 1)
            values[lbl + 'Min'] = Decode.toHumidity_2_0(buf, self.BUFMAP[x][6], 1)
            values[lbl] = Decode.toHumidity_2_0(buf, self.BUFMAP[x][7], 1)
            values[lbl + 'MaxDT'] = None if values[lbl + 'Max'] == SensorLimits.humidity_NP or values[lbl + 'Max'] == SensorLimits.humidity_OFL else Decode.toDateTime8(buf, self.BUFMAP[x][8], 1, lbl + 'Max')
            values[lbl + 'MinDT'] = None if values[lbl + 'Min'] == SensorLimits.humidity_NP or values[lbl + 'Min'] == SensorLimits.humidity_OFL else Decode.toDateTime8(buf, self.BUFMAP[x][9], 1, lbl + 'Min')
        values['AlarmData'] = buf[223:223 + 12]
        self.values = values

    def to_log(self):
        logdbg("timestamp: %s" % self.values['timestamp'])
        logdbg("SignalQuality: %3.0f " % self.values['SignalQuality'])
        for x in range(0, 9):
            if self.values['Temp%d' % x] != SensorLimits.temperature_NP:
                logdbg("Temp%d:     %5.1f   Min: %5.1f (%s)   Max: %5.1f (%s)"
                       % (x, self.values['Temp%s' % x],
                          self.values['Temp%sMin' % x],
                          self.values['Temp%sMinDT' % x],
                          self.values['Temp%sMax' % x],
                          self.values['Temp%sMaxDT' % x]))
            if self.values['Humidity%d' % x] != SensorLimits.humidity_NP:
                logdbg("Humidity%d: %5.0f   Min: %5.0f (%s)   Max: %5.0f (%s)"
                       % (x, self.values['Humidity%s' % x],
                          self.values['Humidity%sMin' % x],
                          self.values['Humidity%sMinDT' % x],
                          self.values['Humidity%sMax' % x],
                          self.values['Humidity%sMaxDT' % x]))
        byte_str = ' '.join(['%02x' % x for x in self.values['AlarmData']])
        logdbg('AlarmData: %s' % byte_str)









class StationConfig(object):

    BUFMAP = {0: ( 8, 11, 14, 17, 20, 23, 26, 29, 32),
              1: ( 9, 12, 15, 18, 21, 24, 27, 30, 33),
              2: (35, 37, 39, 41, 43, 45, 47, 49, 51),
              3: (36, 38, 40, 42, 44, 46, 48, 50, 52),
              4: (58, 66, 74, 82, 90, 98,106,114)}

    def __init__(self):
        self.values = dict()
        self.set_values = dict()
        self.read_config_sensor_texts = True
        self.values['InBufCS'] = 0  # checksum of received config
        self.values['OutBufCS'] = 0  # calculated checksum from outbuf config
        self.values['Settings'] = 0
        self.values['TimeZone'] = 0
        self.values['HistoryInterval'] = 0
        self.values['AlarmSet'] = [0] * 5
        self.values['ResetHiLo'] = 0
        for i in range(0, 9):
            self.values['Temp%dMax' % i] = SensorLimits.temperature_NP
            self.values['Temp%dMin' % i] = SensorLimits.temperature_NP
            self.values['Humidity%dMax' % i] = SensorLimits.humidity_NP
            self.values['Humidity%dMin' % i] = SensorLimits.humidity_NP
        for i in range(1, 9):
            self.values['Description%d' % i] = [0] * 8
            self.values['SensorText%d' % i] = ''
            self.set_values['Description%d' % i] = [0] * 8
            self.set_values['SensorText%d' % i] = ''
    
    def getOutBufCS(self):
        return self.values['OutBufCS']
             
    def getInBufCS(self):
        return self.values['InBufCS']

    def setAlarmClockOffset(self):
        # set Humidity Lo alarm when stations clock is too way off
        self.values['Humidity0Min'] = 99
        self.values['AlarmSet'][4] = (self.values['AlarmSet'][4] & 0xfd) + 0x2

    def resetAlarmClockOffset(self):
        # reset Humidity Lo alarm when stations clock is within margins
        self.values['Humidity0Min'] = 20
        self.values['AlarmSet'][4] = (self.values['AlarmSet'][4] & 0xfd)

    def setSensorText(self, values):
        # test if config is read and sensor texts are not set before
        if self.values['InBufCS'] != 0 and self.read_config_sensor_texts:
            self.read_config_sensor_texts = False
            # Use the sensor labels from the configuration
            for x in range(1, 9):
                lbl = 'sensor_text%d' % x
                self.set_values['SensorText%d' % x] = values[lbl]
                sensor_text = values[lbl]
                if sensor_text is not None:
                    if len(sensor_text) > 10:
                        loginf('sensor_text%d: "%s" is too long, trimmed to'
                               ' 10 characters' % (x, sensor_text))
                        sensor_text = sensor_text[0:10]
                    text_ok = True
                    for y in range(0, len(sensor_text)):
                        if not sensor_text[y:y + 1] in Decode.CHARSTR:
                            text_ok = False
                            loginf('sensor_text%d: "%s" contains bogus'
                                   ' character %s at position %s' %
                                   (x, sensor_text, sensor_text[y:y + 1], y + 1))
                    if not text_ok:
                        sensor_text = None
                if sensor_text is not None:
                    if self.values['SensorText%s' % x] == '(No sensor)':
                        logerr('sensor_text%d: "%s" ignored: no sensor present'
                               % (x, sensor_text))
                    else:
                        logdbg('sensor_text%d: "%s"' % (x, sensor_text))
                        txt = [0] * 8
                        # just for clarity we didn't optimize the code below
                        # map 10 characters of 6 bits into 8 bytes of 8 bits
                        padded_sensor_text = sensor_text.ljust(10, '!')
                        char_id1 = Decode.CHARSTR.find(padded_sensor_text[0:1])
                        char_id2 = Decode.CHARSTR.find(padded_sensor_text[1:2])
                        char_id3 = Decode.CHARSTR.find(padded_sensor_text[2:3])
                        char_id4 = Decode.CHARSTR.find(padded_sensor_text[3:4])
                        char_id5 = Decode.CHARSTR.find(padded_sensor_text[4:5])
                        char_id6 = Decode.CHARSTR.find(padded_sensor_text[5:6])
                        char_id7 = Decode.CHARSTR.find(padded_sensor_text[6:7])
                        char_id8 = Decode.CHARSTR.find(padded_sensor_text[7:8])
                        char_id9 = Decode.CHARSTR.find(padded_sensor_text[8:9])
                        char_id10 = Decode.CHARSTR.find(padded_sensor_text[9:10])
                        txt[7] = ((char_id1 << 6) & 0xC0) + (char_id2 & 0x30) + ((char_id1 >> 2) & 0x0F)
                        txt[6] = ((char_id3 << 2) & 0xF0) + (char_id2 & 0x0F)
                        txt[5] = ((char_id4 << 4) & 0xF0) + ((char_id3 << 2) & 0x0C) + ((char_id4 >> 4) & 0x03)
                        txt[4] = ((char_id5 << 6) & 0xC0) + (char_id6 & 0x30) + ((char_id5 >> 2) & 0x0F)
                        txt[3] = ((char_id7 << 2) & 0xF0) + (char_id6 & 0x0F)
                        txt[2] = ((char_id8 << 4) & 0xF0) + ((char_id7 << 2) & 0x0C) + ((char_id8 >> 4) & 0x03)
                        txt[1] = ((char_id9 << 6) & 0xC0) + (char_id10 & 0x30) + ((char_id9 >> 2) & 0x0F)
                        txt[0] = (char_id10 & 0x0F)
                        # copy the results to the outputbuffer data
                        self.values['Description%d' % x] = txt
                        self.values['SensorText%d' % x] = sensor_text.ljust(10)

    @staticmethod
    def reverseByteOrder(buf, start, count):
        """reverse count bytes in buf beginning at start"""
        for i in range(0, count >> 1):
            tmp = buf[start + i]
            buf[start + i] = buf[start + count - i - 1]
            buf[start + count - i - 1] = tmp
    
    @staticmethod
    def parse_0(number, buf, start, startOnHiNibble, numbytes):
        """Parse 3-digit number with 0 decimals, insert into buf"""
        num = int(number)
        nbuf = [0] * 3
        for i in range(3 - numbytes, 3):
            nbuf[i] = num % 10
            num //= 10
        if startOnHiNibble:
            buf[0 + start] = nbuf[2] * 16 + nbuf[1]
            if numbytes > 2:
                buf[1 + start] = nbuf[0] * 16 + (buf[2 + start] & 0x0F)
        else:
            buf[0 + start] = (buf[0 + start] & 0xF0) + nbuf[2]
            if numbytes > 2:
                buf[1 + start] = nbuf[1] * 16 + nbuf[0]

    @staticmethod
    def parse_1(number, buf, start, startOnHiNibble, numbytes):
        """Parse 3 digit number with 1 decimal, insert into buf"""
        StationConfig.parse_0(number * 10.0, buf, start, startOnHiNibble, numbytes)

    def read(self, buf):
        values = dict()
        values['Settings'] = buf[5]
        values['TimeZone'] = buf[6]
        values['HistoryInterval'] = buf[7] & 0xF
        for x in range(0, 9):
            lbl = 'Temp%s' % x
            values[lbl + 'Max'] = Decode.toTemperature_3_1(buf, self.BUFMAP[0][x], 1)
            values[lbl + 'Min'] = Decode.toTemperature_3_1(buf, self.BUFMAP[1][x], 0)
            lbl = 'Humidity%s' % x
            values[lbl + 'Max'] = Decode.toHumidity_2_0(buf, self.BUFMAP[2][x], 1)
            values[lbl + 'Min'] = Decode.toHumidity_2_0(buf, self.BUFMAP[3][x], 1)
        values['AlarmSet'] = buf[53:53 + 5]
        for x in range(1, 9):
            values['Description%s' % x] = buf[self.BUFMAP[4][x - 1]:self.BUFMAP[4][x - 1] + 8]
            txt1 = Decode.toCharacters3_2(buf, self.BUFMAP[4][x - 1] + 6, 0)
            txt2 = Decode.toCharacters3_2(buf, self.BUFMAP[4][x - 1] + 5, 1)
            txt3 = Decode.toCharacters3_2(buf, self.BUFMAP[4][x - 1] + 3, 0)
            txt4 = Decode.toCharacters3_2(buf, self.BUFMAP[4][x - 1] + 2, 1)
            txt5 = Decode.toCharacters3_2(buf, self.BUFMAP[4][x - 1], 0)
            sensor_txt = txt1 + txt2 + txt3 + txt4 + txt5
            if sensor_txt == ' E@@      ':
                values['SensorText%s' % x] = '(No sensor)'
            else:
                values['SensorText%s' % x] = sensor_txt
        values['ResetHiLo'] = buf[122]
        values['InBufCS'] = (buf[123] << 8) | buf[124]
        # checksum is not calculated for ResetHiLo (Output only)
        values['OutBufCS'] = calc_checksum(buf, 5, end=122) + 7
        self.values = values

    # FIXME: this has side effects that should be removed
    # FIXME: self.values['HistoryInterval']
    # FIXME: self.values['OutBufCS']
    def testConfigChanged(self):
        """see if configuration has changed"""
        newbuf = [0] * 125
        # Set historyInterval to 5 minutes if > 5 minutes (default: 15 minutes)
        if self.values['HistoryInterval'] > HI_05MIN:
            logdbg('change HistoryInterval to 5 minutes')
            self.values['HistoryInterval'] = HI_05MIN
        newbuf[5] = self.values['Settings']
        newbuf[6] = self.values['TimeZone']
        newbuf[7] = self.values['HistoryInterval']
        for x in range(0, 9):
            lbl = 'Temp%s' % x
            self.parse_1(self.values[lbl + 'Max'] + SensorLimits.temperature_offset, newbuf, self.BUFMAP[0][x], 1, 3)
            self.parse_1(self.values[lbl + 'Min'] + SensorLimits.temperature_offset, newbuf, self.BUFMAP[1][x], 0, 3)
            self.reverseByteOrder(newbuf, self.BUFMAP[0][x], 3)  # Temp
            lbl = 'Humidity%s' % x
            self.parse_0(self.values[lbl + 'Max'], newbuf, self.BUFMAP[2][x], 1, 2)
            self.parse_0(self.values[lbl + 'Min'], newbuf, self.BUFMAP[3][x], 1, 2)
            self.reverseByteOrder(newbuf, self.BUFMAP[2][x], 2)  # Humidity
        # insert reverse self.values['AlarmSet'] into newbuf
        rev = self.values['AlarmSet'][::-1]
        for y in range(0, 5):
            newbuf[53 + y] = rev[y]
        # insert reverse self.values['Description%d'] into newbuf
        for x in range(1, 9):
            rev = self.values['Description%d' % x][::-1]
            for y in range(0, 8):
                newbuf[self.BUFMAP[4][x - 1] + y] = rev[y]
        newbuf[122] = self.values['ResetHiLo']
        # checksum is not calculated for ResetHiLo (Output only)
        self.values['OutBufCS'] = calc_checksum(newbuf, 5, end=122) + 7
        newbuf[123] = (self.values['OutBufCS'] >> 8) & 0xFF
        newbuf[124] = (self.values['OutBufCS'] >> 0) & 0xFF
        if self.values['OutBufCS'] == self.values['InBufCS']:
            if DEBUG_CONFIG_DATA > 2:
                logdbg('checksum not changed: OutBufCS=%04x' %
                       self.values['OutBufCS'])
            changed = 0
        else:
            if DEBUG_CONFIG_DATA > 0:
                logdbg('checksum changed: OutBufCS=%04x InBufCS=%04x ' % 
                       (self.values['OutBufCS'], self.values['InBufCS']))
            if self.values['InBufCS'] != 0 and DEBUG_CONFIG_DATA > 1:
                self.to_log()
            changed = 1
        return changed, newbuf

    def to_log(self):
        contrast = (int(self.values['Settings']) >> 4) & 0x0F
        alert = 'ON' if int(self.values['Settings']) & 0x8 == 0 else 'OFF'
        dcf_recep = 'OFF' if int(self.values['Settings']) & 0x4 == 0 else 'ON'
        time_form = '24h' if int(self.values['Settings']) & 0x2 == 0 else '12h'
        temp_form = 'C' if int(self.values['Settings']) & 0x1 == 0 else 'F'
        time_zone = self.values['TimeZone'] if int(self.values['TimeZone']) <= 12 else int(self.values['TimeZone']) - 256
        history_interval = history_intervals.get(self.values['HistoryInterval'])
        logdbg('OutBufCS: %04x' % self.values['OutBufCS'])
        logdbg('InBufCS:  %04x' % self.values['InBufCS'])
        logdbg('Settings: %02x: contrast: %s, alert: %s, DCF reception: %s, time format: %s, temp format: %s' %
               (self.values['Settings'], contrast, alert, dcf_recep, time_form, temp_form))
        logdbg('TimeZone difference with Frankfurt (CET): %02x (tz: %s hour)' % (self.values['TimeZone'], time_zone))
        logdbg('HistoryInterval: %02x, period: %s minute(s)' % (self.values['HistoryInterval'], history_interval))
        byte_str = ' '.join(['%02x' % x for x in self.values['AlarmSet']])
        logdbg('AlarmSet:     %s' % byte_str)
        logdbg('ResetHiLo:    %02x' % self.values['ResetHiLo'])
        for x in range(0, 9):
            logdbg('Sensor%d:      %3.1f - %3.1f, %3.0f - %3.0f' %
                   (x,
                    self.values['Temp%dMin' % x], 
                    self.values['Temp%dMax' % x],
                    self.values['Humidity%dMin' % x],
                    self.values['Humidity%dMax' % x]))
        for x in range(1, 9):
            byte_str = ' '.join(['%02x' % y for y in self.values['Description%d' % x]])
            logdbg('Description%d: %s; SensorText%d: %s' % (x, byte_str, x, self.values['SensorText%s' % x]))

    def as_dict(self):
        return {'checksum_in': self.values['InBufCS'],
                'checksum_out': self.values['OutBufCS'],
                'settings': self.values['Settings'],
                'history_interval': self.values['HistoryInterval']}









class TransceiverSettings(object): 
    def __init__(self):
        self.serial_number = None
        self.device_id = None


class LastStat(object):
    def __init__(self):
        self.last_link_quality = None
        self.last_history_index = None
        self.latest_history_index = None
        self.last_seen_ts = None
        self.last_weather_ts = 0
        self.last_history_ts = 0
        self.last_config_ts = 0

    def update(self, seen_ts=None, quality=None,
               weather_ts=None, history_ts=None, config_ts=None):
        if DEBUG_COMM > 1:
            logdbg('LastStat: seen=%s quality=%s weather=%s history=%s config=%s' %
                   (seen_ts, quality, weather_ts, history_ts, config_ts))
        if seen_ts is not None:
            self.last_seen_ts = seen_ts
        if quality is not None:
            self.last_link_quality = quality
        if weather_ts is not None:
            self.last_weather_ts = weather_ts
        if history_ts is not None:
            self.last_history_ts = history_ts
        if config_ts is not None:
            self.last_config_ts = config_ts








class Transceiver(object):
    """USB dongle abstraction"""

    def __init__(self):
        self.devh = None
        self.timeout = 1000
        self.last_dump = None

    def open(self, vid, pid, serial):
        device = Transceiver._find_device(vid, pid, serial)
        if device is None:
            logerr('Cannot find USB device with Vendor=0x%04x ProdID=0x%04x Serial=%s' % 
                   (vid, pid, serial))
            raise NameError('Unable to find transceiver on USB')
        self.devh = self._open_device(device)

    def close(self):
        Transceiver._close_device(self.devh)
        self.devh = None

    @staticmethod
    def _find_device(vid, pid, serial):
        for bus in usb.busses():
            for dev in bus.devices:
                if dev.idVendor == vid and dev.idProduct == pid:
                    if serial is None:
                        loginf('found transceiver at bus={} device={}'.format(
                               bus.dirname, dev.filename))
                        return dev
                    else:
                        sn = Transceiver._read_serial(dev)
                        if str(serial) == sn:
                            loginf('found transceiver at bus=%s device=%s serial=%s' %
                                   (bus.dirname, dev.filename, sn))
                            return dev
                        else:
                            loginf('skipping transceiver with serial %s (looking for %s)' %
                                   (sn, serial))
        return None

    @staticmethod
    def _read_serial(dev):
        handle = None
        try:
            # see if we can read the serial without claiming the interface.
            # we do not want to disrupt any process that might already be
            # using the device.
            handle = Transceiver._open_device(dev)
            buf = Transceiver.readCfg(handle, 0x1F9, 7)
            if buf:
                return ''.join(['%02d' % x for x in buf[0:7]])
        except usb.USBError as e:
            logerr("cannot read serial number: %s" % e)
        finally:
            # if we claimed the interface, we must release it
            Transceiver._close_device(handle)
            # FIXME: not sure whether we must delete the handle
#            if handle is not None:
#                del handle
        return None

    @staticmethod
    def _open_device(dev, interface=0):
        handle = dev.open()
        if not handle:
            raise NameError('Open USB device failed')

        loginf('manufacturer: %s' % handle.getString(dev.iManufacturer, 30))
        loginf('product: %s' % handle.getString(dev.iProduct, 30))
        loginf('interface: %d' % interface)

        # be sure kernel does not claim the interface
        try:
            handle.detachKernelDriver(interface)
        except usb.USBError:
            pass

        # attempt to claim the interface
        try:
            logdbg('claiming USB interface %d' % interface)
            handle.claimInterface(interface)
            handle.setAltInterface(interface)
        except usb.USBError as e:
            Transceiver._close_device(handle)
            logerr('Unable to claim USB interface %s: %s' % (interface, e))
            raise NameError(e)

        # FIXME: check return values
        usb_wait = 0.05
        handle.getDescriptor(0x1, 0, 0x12)
        time.sleep(usb_wait)
        handle.getDescriptor(0x2, 0, 0x9)
        time.sleep(usb_wait)
        handle.getDescriptor(0x2, 0, 0x22)
        time.sleep(usb_wait)
        handle.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                          0xa, [], 0x0, 0x0, 1000)
        time.sleep(usb_wait)
        handle.getDescriptor(0x22, 0, 0x2a9)
        time.sleep(usb_wait)
        return handle

    @staticmethod
    def _close_device(handle):
        if handle is not None:
            try:
                logdbg('releasing USB interface')
                handle.releaseInterface()
            except usb.USBError:
                pass

    def setTX(self):
        buf = [0] * 0x15
        buf[0] = 0xD1
        if DEBUG_COMM > 1:
            self.dump('setTX', buf, fmt=DEBUG_DUMP_FORMAT)
        self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                             request=0x0000009,
                             buffer=buf,
                             value=0x00003d1,
                             index=0x0000000,
                             timeout=self.timeout)

    def setRX(self):
        buf = [0] * 0x15
        buf[0] = 0xD0
        if DEBUG_COMM > 1:
            self.dump('setRX', buf, fmt=DEBUG_DUMP_FORMAT)
        self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                             request=0x0000009,
                             buffer=buf,
                             value=0x00003d0,
                             index=0x0000000,
                             timeout=self.timeout)

    def getState(self):
        buf = self.devh.controlMsg(
            requestType=usb.TYPE_CLASS | usb.RECIP_INTERFACE | usb.ENDPOINT_IN,
            request=usb.REQ_CLEAR_FEATURE,
            buffer=0x0a,
            value=0x00003de,
            index=0x0000000,
            timeout=self.timeout)
        if DEBUG_COMM > 1:
            self.dump('getState', buf, fmt=DEBUG_DUMP_FORMAT)
        return buf[1:3]

    def readConfigFlash(self, addr, nbytes):
        new_data = [0] * 0x15
        while nbytes:
            buf = [0xcc] * 0x0f  # 0x15
            buf[0] = 0xdd
            buf[1] = 0x0a
            buf[2] = (addr >> 8) & 0xFF
            buf[3] = (addr >> 0) & 0xFF
            if DEBUG_COMM > 1:
                self.dump('readCfgFlash>', buf, fmt=DEBUG_DUMP_FORMAT)
            self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                                 request=0x0000009,
                                 buffer=buf,
                                 value=0x00003dd,
                                 index=0x0000000,
                                 timeout=self.timeout)
            buf = self.devh.controlMsg(
                usb.TYPE_CLASS | usb.RECIP_INTERFACE | usb.ENDPOINT_IN,
                request=usb.REQ_CLEAR_FEATURE,
                buffer=0x15,
                value=0x00003dc,
                index=0x0000000,
                timeout=self.timeout)
            new_data = [0] * 0x15
            if nbytes < 16:
                for i in range(0, nbytes):
                    new_data[i] = buf[i + 4]
                nbytes = 0
            else:
                for i in range(0, 16):
                    new_data[i] = buf[i + 4]
                nbytes -= 16
                addr += 16
            if DEBUG_COMM > 1:
                self.dump('readCfgFlash<', buf, fmt=DEBUG_DUMP_FORMAT)
        return new_data

    def setState(self, state):
        buf = [0] * 0x15
        buf[0] = 0xd7
        buf[1] = state
        if DEBUG_COMM > 1:
            self.dump('setState', buf, fmt=DEBUG_DUMP_FORMAT)
        self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                             request=0x0000009,
                             buffer=buf,
                             value=0x00003d7,
                             index=0x0000000,
                             timeout=self.timeout)

    def setFrame(self, nbytes, data):
        buf = [0] * 0x111
        buf[0] = 0xd5
        buf[1] = nbytes >> 8
        buf[2] = nbytes
        for i in range(0, nbytes):
            buf[i + 3] = data[i]
        if DEBUG_COMM == 1:
            self.dump('setFrame', buf, 'short')
        elif DEBUG_COMM > 1:
            self.dump('setFrame', buf, fmt=DEBUG_DUMP_FORMAT)
        self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                             request=0x0000009,
                             buffer=buf,
                             value=0x00003d5,
                             index=0x0000000,
                             timeout=self.timeout)

    def getFrame(self):
        buf = self.devh.controlMsg(
            usb.TYPE_CLASS | usb.RECIP_INTERFACE | usb.ENDPOINT_IN,
            request=usb.REQ_CLEAR_FEATURE,
            buffer=0x111,
            value=0x00003d6,
            index=0x0000000,
            timeout=self.timeout)
        data = [0] * 0x131
        nbytes = (buf[1] << 8 | buf[2]) & 0x1ff
        for i in range(0, nbytes):
            data[i] = buf[i + 3]
        if DEBUG_COMM == 1:
            self.dump('getFrame', buf, 'short')
        elif DEBUG_COMM > 1:
            self.dump('getFrame', buf, fmt=DEBUG_DUMP_FORMAT)
        return nbytes, data

    def writeReg(self, regAddr, data):
        buf = [0] * 0x05
        buf[0] = 0xf0
        buf[1] = regAddr & 0x7F
        buf[2] = 0x01
        buf[3] = data
        buf[4] = 0x00
        if DEBUG_COMM > 1:
            self.dump('writeReg', buf, fmt=DEBUG_DUMP_FORMAT)
        self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                             request=0x0000009,
                             buffer=buf,
                             value=0x00003f0,
                             index=0x0000000,
                             timeout=self.timeout)

    def execute(self, command):
        buf = [0] * 0x0f  # 0x15
        buf[0] = 0xd9
        buf[1] = command
        if DEBUG_COMM > 1:
            self.dump('execute', buf, fmt=DEBUG_DUMP_FORMAT)
        self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                             request=0x0000009,
                             buffer=buf,
                             value=0x00003d9,
                             index=0x0000000,
                             timeout=self.timeout)

    def setPreamblePattern(self, pattern):
        buf = [0] * 0x15
        buf[0] = 0xd8
        buf[1] = pattern
        if DEBUG_COMM > 1:
            self.dump('setPreamble', buf, fmt=DEBUG_DUMP_FORMAT)
        self.devh.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                             request=0x0000009,
                             buffer=buf,
                             value=0x00003d8,
                             index=0x0000000,
                             timeout=self.timeout)

    # three formats, long, short, auto.  short shows only the first 16 bytes.
    # long shows the full length of the buffer.  auto shows the message length
    # as indicated by the length in the message itself for setFrame and
    # getFrame, or the first 16 bytes for any other message.
    def dump(self, cmd, buf, fmt='auto', length=301):
        strbuf = ''
        if fmt == 'auto':
            if buf[0] in [0xd5, 0x00]:
                msglen = buf[2] + 3        # use msg length for set/get frame
            else:
                msglen = 16                # otherwise do same as short format
        elif fmt == 'short':
            msglen = 16
        else:
            msglen = length                # dedicated 'long' length
        for i, x in enumerate(buf):
            strbuf += str('%02x ' % x)
            if (i + 1) % 16 == 0:
                self.dumpstr(cmd, strbuf)
                strbuf = ''
            if msglen is not None and i + 1 >= msglen:
                break
        if strbuf:
            self.dumpstr(cmd, strbuf)

    # filter output that we do not care about, pad the command string.
    def dumpstr(self, cmd, strbuf):
        pad = ' ' * (15 - len(cmd))
        # de15 is idle, de14 is intermediate
        if strbuf in ['de 15 00 00 00 00 ', 'de 14 00 00 00 00 ']:
            if strbuf != self.last_dump or DEBUG_COMM > 2:
                logdbg('%s: %s%s' % (cmd, pad, strbuf))
            self.last_dump = strbuf
        else:
            logdbg('%s: %s%s' % (cmd, pad, strbuf))
            self.last_dump = None

    @staticmethod
    def readCfg(handle, addr, nbytes, timeout=1000):
        new_data = [0] * 0x15
        while nbytes:
            buf = [0xcc] * 0x0f  # 0x15
            buf[0] = 0xdd
            buf[1] = 0x0a
            buf[2] = (addr >> 8) & 0xFF
            buf[3] = (addr >> 0) & 0xFF
            handle.controlMsg(usb.TYPE_CLASS + usb.RECIP_INTERFACE,
                              request=0x0000009,
                              buffer=buf,
                              value=0x00003dd,
                              index=0x0000000,
                              timeout=timeout)
            buf = handle.controlMsg(
                usb.TYPE_CLASS | usb.RECIP_INTERFACE | usb.ENDPOINT_IN,
                request=usb.REQ_CLEAR_FEATURE,
                buffer=0x15,
                value=0x00003dc,
                index=0x0000000,
                timeout=timeout)
            new_data = [0] * 0x15
            if nbytes < 16:
                for i in range(0, nbytes):
                    new_data[i] = buf[i + 4]
                nbytes = 0
            else:
                for i in range(0, 16):
                    new_data[i] = buf[i + 4]
                nbytes -= 16
                addr += 16
        return new_data


if __name__ == '__main__':
    # kldr = KlimaLoggConfigurator()
    kldr = KlimaLoggDriver()
    kldr.clear_wait_at_start()
  #  kldr.show_history(5)
    for packet in kldr.genLoopPackets():
        print("Time: {} - {}".format(packet['dateTime'], packet))
        time.sleep(5)
    