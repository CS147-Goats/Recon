#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <Preferences.h>
#include <Wire.h>
#include <Adafruit_MPU6050.h>
#include <HCSR04.h>
#include <ArduinoJson.h>
#include <math.h>
#include <strings.h>
#include <time.h>
#include "iot_common.h"
#include "iot_display.h"
#include "iot_provision.h"

// ---------------- Team settings (not secret) ----------------
#define TEAM_ID "recon"   // your team number from Canvas, e.g. "T07"

constexpr uint16_t MQTT_PORT        = 8883;    // MQTT over TLS
constexpr uint16_t MQTT_KEEPALIVE_S = 15;      // broker declares us dead after ~1.5 x this
constexpr uint32_t WIFI_RETRY_MS    = 20000;   // call WiFi.begin() again if still down
constexpr uint32_t BACKOFF_START_MS = 1000;    // first MQTT retry after 1 s ...
constexpr uint32_t BACKOFF_MAX_MS   = 30000;   // ... doubling up to 30 s

constexpr uint32_t SENSOR_POLL_MS = 250;
constexpr uint32_t ENVIRONMENT_PERIOD_MS = 5UL * 60UL * 1000UL;
constexpr float DISTANCE_EVENT_THRESHOLD_MM = 100.0f;
constexpr float ACCELERATION_EVENT_THRESHOLD_MPS2 = 3.0f;

// ---------------- Globals ----------------
struct Credentials {
  String wifiSsid, wifiPass;   // network
  String mqttHost;             // e.g. abc123.s1.eu.hivemq.cloud (no "mqtts://", no port)
  String mqttUser, mqttPass;   // HiveMQ Cloud credential (Access Management tab)
};
Credentials creds;

Preferences prefs;
constexpr const char* NVS_NAMESPACE = "iot-lab5";   // max 15 characters

WiFiClientSecure net;          // TLS socket
PubSubClient mqtt(net);        // MQTT client running on top of the TLS socket
Adafruit_MPU6050 mpu;

const String TOPIC_EVENTS = String("iot/") + TEAM_ID + "/events";
const String TOPIC_ENVIRONMENT = String("iot/") + TEAM_ID + "/environment";
const String TOPIC_CMD = String("iot/") + TEAM_ID + "/cmd";
const String TOPIC_STATUS    = String("iot/") + TEAM_ID + "/status";

String clientId;               // "iot-T00-<chip id>", unique per board

bool mpuOk = false;
bool armed = false;
bool alertActive = false;
float distanceMm = NAN;
float accelerationMagnitude = NAN;

uint32_t backoffMs   = BACKOFF_START_MS;
uint32_t lastMqttTry = 0;
uint32_t seqNo       = 0;      // message counter: gaps in seq = lost messages
uint32_t pubOk = 0, pubFail = 0, reconnects = 0;

void mqttCallback(char* topic, byte* payload, unsigned int length);

// ============================================================================================
// Part B: credentials in NVS (Preferences)
// ============================================================================================

// Load credentials from NVS into `creds`. Returns true if the required ones are present.
bool loadCredentials() {
  // TODO 1: open the NVS namespace READ-ONLY: prefs.begin(NVS_NAMESPACE, true);
  //         read the five keys into creds with prefs.getString("key", ""):
  //           "wifi_ssid", "wifi_pass", "mqtt_host", "mqtt_user", "mqtt_pass"
  //         then prefs.end(). Return true only if wifi_ssid, mqtt_host and mqtt_user are
  //         not empty (an open Wi-Fi network may have an empty password).
  prefs.begin(NVS_NAMESPACE, true);
  creds.wifiSsid = prefs.getString("wifi_ssid", "");
  creds.wifiPass = prefs.getString("wifi_pass", "");
  creds.mqttHost = prefs.getString("mqtt_host", "");
  creds.mqttUser = prefs.getString("mqtt_user", "");
  creds.mqttPass = prefs.getString("mqtt_pass", "");
  prefs.end();
  return creds.wifiSsid != "" && creds.mqttHost != "" && creds.mqttUser != "";
}

// Write `creds` to NVS.
void saveCredentials() {
  // TODO 2: open the namespace read-write (prefs.begin(NVS_NAMESPACE, false)), store the
  //         five values with prefs.putString("key", value) using the SAME key names as
  //         in TODO 1, then prefs.end().
  prefs.begin(NVS_NAMESPACE, false);
  prefs.putString("wifi_ssid", creds.wifiSsid);
  prefs.putString("wifi_pass", creds.wifiPass);
  prefs.putString("mqtt_host", creds.mqttHost);
  prefs.putString("mqtt_user", creds.mqttUser);
  prefs.putString("mqtt_pass", creds.mqttPass);
  prefs.end();
}

// Erase every key in our namespace (FORGET command). Given.
void forgetCredentials() {
  prefs.begin(NVS_NAMESPACE, false);
  prefs.clear();
  prefs.end();
  creds = Credentials();
  Serial.println("Credentials erased from NVS.");
}

void printCredentials() {
  Serial.print("  Wi-Fi SSID    : "); Serial.println(creds.wifiSsid);
  Serial.print("  Wi-Fi password: "); Serial.println(iotMasked(creds.wifiPass));
  Serial.print("  MQTT host     : "); Serial.println(creds.mqttHost);
  Serial.print("  MQTT username : "); Serial.println(creds.mqttUser);
  Serial.print("  MQTT password : "); Serial.println(iotMasked(creds.mqttPass));
}

// Ask for every value over Serial and save it. Blocking on purpose (setup / on request). Given.
void runProvisioning() {
  Serial.println();
  Serial.println("=== Provisioning ===");
  Serial.println("Values are stored in this board's NVS flash, never in your source code.");
  Serial.println("Type each value and press Enter.");
  do {
    creds.wifiSsid = iotAsk("Wi-Fi SSID:");
  } while (creds.wifiSsid.length() == 0);
  creds.wifiPass = iotAsk("Wi-Fi password (leave empty for an open network):");
  do {
    creds.mqttHost = iotCleanHost(iotAsk("MQTT host (HiveMQ Cluster URL, e.g. abc123.s1.eu.hivemq.cloud):"));
  } while (creds.mqttHost.length() == 0);
  do {
    creds.mqttUser = iotAsk("MQTT username:");
  } while (creds.mqttUser.length() == 0);
  creds.mqttPass = iotAsk("MQTT password:");

  saveCredentials();
  Serial.println("Saved to NVS:");
  printCredentials();
  Serial.println("Tip: type FORGET at startup to erase them (do this before you return the kit).");
}

// ============================================================================================
// Part C: MQTT over TLS
// ============================================================================================

void configureMqtt() {
  // TODO 3: set up the TLS socket and the MQTT client.
  //   (a) net.setInsecure();  with a LOUD comment saying what it means (see the handout).
  //   (b) mqtt.setServer(creds.mqttHost.c_str(), MQTT_PORT);
  //       (PubSubClient keeps the POINTER, so creds.mqttHost must not change afterwards.)
  //   (c) mqtt.setKeepAlive(MQTT_KEEPALIVE_S); mqtt.setSocketTimeout(5); mqtt.setBufferSize(512);
  
  // CONNECTION IS ENCRYPTED, BUT WE'RE NOT VERIFYING WHO THE MQTT BROKER IS
  net.setInsecure();
  mqtt.setServer(creds.mqttHost.c_str(), MQTT_PORT);   // 8883
  mqtt.setKeepAlive(MQTT_KEEPALIVE_S);                 // 15 s
  mqtt.setSocketTimeout(5);
  mqtt.setBufferSize(512);
  mqtt.setCallback(mqttCallback);
}

const char* mqttStateText(int s) {
  switch (s) {
    case MQTT_CONNECTION_TIMEOUT:      return "-4 CONNECTION_TIMEOUT (no answer: host/port/firewall?)";
    case MQTT_CONNECTION_LOST:         return "-3 CONNECTION_LOST (network dropped)";
    case MQTT_CONNECT_FAILED:          return "-2 CONNECT_FAILED (TCP/TLS failed: host name? Wi-Fi blocks 8883?)";
    case MQTT_DISCONNECTED:            return "-1 DISCONNECTED";
    case MQTT_CONNECTED:               return "0 CONNECTED";
    case MQTT_CONNECT_BAD_PROTOCOL:    return "1 BAD_PROTOCOL";
    case MQTT_CONNECT_BAD_CLIENT_ID:   return "2 BAD_CLIENT_ID";
    case MQTT_CONNECT_UNAVAILABLE:     return "3 UNAVAILABLE (broker busy)";
    case MQTT_CONNECT_BAD_CREDENTIALS: return "4 BAD_CREDENTIALS (username/password)";
    case MQTT_CONNECT_UNAUTHORIZED:    return "5 UNAUTHORIZED (credential lacks permission)";
    default:                           return "unknown";
  }
}

bool publishStatus(const char* status) {
  char payload[64];
  snprintf(payload, sizeof(payload), "{\"seq\":%lu,\"status\":\"%s\"}",
           (unsigned long)seqNo++, status);
  return mqtt.publish(TOPIC_STATUS.c_str(), payload, true);
}

// One connection attempt. Returns true when connected and subscribed.
bool mqttConnect() {
  Serial.print("MQTT: connecting to ");
  Serial.print(creds.mqttHost);
  Serial.print(":");
  Serial.print(MQTT_PORT);
  Serial.print(" as ");
  Serial.println(clientId);

  char willPayload[64];
  snprintf(willPayload, sizeof(willPayload), "{\"seq\":%lu,\"status\":\"offline\"}",
           (unsigned long)seqNo++);
  if (!mqtt.connect(clientId.c_str(), creds.mqttUser.c_str(), creds.mqttPass.c_str(),
                    TOPIC_STATUS.c_str(), 1, true, willPayload)) {
    return false;
  }
  if (!mqtt.subscribe(TOPIC_CMD.c_str(), 1)) {
    Serial.println("MQTT: failed to subscribe to command topic");
    mqtt.disconnect();
    return false;
  }
  if (!publishStatus("online")) {
    Serial.println("MQTT: failed to publish online status");
  }
  return true;
}

// Called every pass of loop(). Never blocks for long, except inside one connect attempt.
void maintainWiFi() {
  static uint32_t lastBegin = 0;
  static bool started = false, wasUp = false;
  const bool up = (WiFi.status() == WL_CONNECTED);
  if (up && !wasUp) {
    Serial.print("Wi-Fi: connected, IP "); Serial.println(WiFi.localIP());
    configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  }
  if (!up && wasUp) Serial.println("Wi-Fi: connection lost");
  wasUp = up;
  if (!up && (!started || millis() - lastBegin >= WIFI_RETRY_MS)) {
    started = true;
    lastBegin = millis();
    Serial.print("Wi-Fi: connecting to ");
    Serial.println(creds.wifiSsid);
    WiFi.begin(creds.wifiSsid.c_str(), creds.wifiPass.c_str());
  }
}

void maintainMqtt() {
  if (WiFi.status() != WL_CONNECTED || mqtt.connected()) return;

  // TODO 5: reconnect with exponential backoff.
  //   If less than backoffMs has passed since lastMqttTry, return (do NOT block).
  //   Otherwise remember the time, call mqttConnect().
  //     success -> print "MQTT: connected", reset backoffMs to BACKOFF_START_MS, reconnects++
  //     failure -> print mqttStateText(mqtt.state()), double backoffMs (max BACKOFF_MAX_MS)
  //                and add a little jitter, e.g. + (millis() % 500), so 40 teams do not all
  //                retry at the same instant after the router reboots.
  if(millis() - lastMqttTry < backoffMs) {
    return;
  }
  lastMqttTry = millis();
  if(mqttConnect()) {
    Serial.println("MQTT: connected");
    backoffMs = BACKOFF_START_MS;
    reconnects++;
  }
  else {
    Serial.println(mqttStateText(mqtt.state()));
    backoffMs = min(backoffMs * 2, BACKOFF_MAX_MS) + (millis() % 500);
  }
}

float readDistanceMm() {
  const float reading = HCSR04.measureDistanceMm()[0];
  return isfinite(reading) && reading > 0.0f ? reading : NAN;
}

bool publishEvent() {
  if (!mqtt.connected()) return false;

  const time_t now = time(nullptr);
  const unsigned long timestamp =
      now >= 1577836800 ? (unsigned long)now : (unsigned long)(millis() / 1000);
  char payload[80];
  snprintf(payload, sizeof(payload), "{\"seq\":%lu,\"timestamp\":%lu}",
           (unsigned long)seqNo++, timestamp);
  const bool published = mqtt.publish(TOPIC_EVENTS.c_str(), payload, false);
  if (published) alertActive = true;
  return published;
}

bool publishEnvironment() {
  if (!mqtt.connected()) return false;

  char distance[20] = "null";
  char acceleration[20] = "null";
  if (isfinite(distanceMm)) {
    snprintf(distance, sizeof(distance), "%.2f", distanceMm);
  }
  if (isfinite(accelerationMagnitude)) {
    snprintf(acceleration, sizeof(acceleration), "%.3f", accelerationMagnitude);
  }

  char payload[112];
  snprintf(payload, sizeof(payload),
           "{\"seq\":%lu,\"distance\":%s,\"acceleration\":%s}",
           (unsigned long)seqNo++, distance, acceleration);
  return mqtt.publish(TOPIC_ENVIRONMENT.c_str(), payload, false);
}

void handleMqttCommand(const char* command) {
  if (strcasecmp(command, "arm") == 0) {
    armed = true;
    alertActive = false;
    Serial.println("Command: device armed");
  } else if (strcasecmp(command, "disarm") == 0) {
    armed = false;
    alertActive = false;
    Serial.println("Command: device disarmed");
  } else if (strcasecmp(command, "dismiss") == 0) {
    alertActive = false;
    Serial.println("Command: alert dismissed");
  } else {
    Serial.print("MQTT: ignored unknown command: ");
    Serial.println(command);
  }
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  if (strcmp(topic, TOPIC_CMD.c_str()) != 0) return;

  StaticJsonDocument<128> document;
  const DeserializationError error = deserializeJson(document, payload, length);
  if (error) {
    Serial.print("MQTT: invalid command JSON: ");
    Serial.println(error.c_str());
    return;
  }
  if (!document["cmd"].is<const char*>()) {
    Serial.println("MQTT: command JSON must include a string 'cmd'");
    return;
  }
  handleMqttCommand(document["cmd"].as<const char*>());
}

void pollSensors() {
  static float previousDistanceMm = NAN;
  static float previousAx = NAN;
  static float previousAy = NAN;
  static float previousAz = NAN;

  distanceMm = readDistanceMm();
  if (isfinite(distanceMm)) {
    if (isfinite(previousDistanceMm) &&
        fabsf(distanceMm - previousDistanceMm) >= DISTANCE_EVENT_THRESHOLD_MM &&
        armed && !alertActive) {
      if (publishEvent()) pubOk++; else pubFail++;
    }
    previousDistanceMm = distanceMm;
  }

  if (!mpuOk) return;
  sensors_event_t acceleration, gyro, temperature;
  mpu.getEvent(&acceleration, &gyro, &temperature);
  const float ax = acceleration.acceleration.x;
  const float ay = acceleration.acceleration.y;
  const float az = acceleration.acceleration.z;
  accelerationMagnitude = sqrtf(ax * ax + ay * ay + az * az);

  if (isfinite(previousAx)) {
    const float dx = ax - previousAx;
    const float dy = ay - previousAy;
    const float dz = az - previousAz;
    const float change = sqrtf(dx * dx + dy * dy + dz * dz);
    if (change >= ACCELERATION_EVENT_THRESHOLD_MPS2 &&
        armed && !alertActive) {
      if (publishEvent()) pubOk++; else pubFail++;
    }
  }
  previousAx = ax;
  previousAy = ay;
  previousAz = az;
}

// ============================================================================================
// Serial console (given)
// ============================================================================================

void printHelp() {
  Serial.println("Commands: HELP | STATUS | PUB (environment now) | PROVISION | FORGET");
}

void printStatus() {
  Serial.print("Team "); Serial.print(TEAM_ID); Serial.print("  client id "); Serial.println(clientId);
  printCredentials();
  Serial.print("  Wi-Fi: "); Serial.println(WiFi.status() == WL_CONNECTED ? "connected" : "down");
  Serial.print("  MQTT : "); Serial.println(mqttStateText(mqtt.state()));
  Serial.print("  published ok/failed: "); Serial.print(pubOk); Serial.print("/"); Serial.print(pubFail);
  Serial.print("  reconnects: "); Serial.println(reconnects);
  Serial.print("  environment period: "); Serial.print(ENVIRONMENT_PERIOD_MS); Serial.println(" ms");
  Serial.print("  armed: "); Serial.println(armed ? "yes" : "no");
}

// A clean DISCONNECT tells the broker "I left on purpose", so it does NOT send the Last Will.
// That is why we publish "offline" ourselves before a planned disconnect.
void goOfflineCleanly() {
  if (mqtt.connected()) {
    if (!publishStatus("offline")) {
      Serial.println("MQTT: failed to publish offline status");
    }
    mqtt.disconnect();
  }
}

void handleCommand(String line) {
  line.trim();
  line.toUpperCase();
  if (line.length() == 0) return;
  if (line == "HELP")   { printHelp(); return; }
  if (line == "STATUS") { printStatus(); return; }
  if (line == "PUB") {
    if (!mqtt.connected()) { Serial.println("ERR not connected to the broker"); return; }
    if (publishEnvironment()) pubOk++; else pubFail++;
    return;
  }
  if (line == "FORGET") { goOfflineCleanly(); forgetCredentials(); Serial.println("Rebooting..."); ESP.restart(); return; }
  if (line == "PROVISION") { goOfflineCleanly(); runProvisioning(); Serial.println("Rebooting..."); ESP.restart(); return; }
  Serial.println("ERR unknown command. Type HELP");
}

void pollConsole() {
  static String buf;
  while (Serial.available()) {
    char c = (char)Serial.read();
    if (c == '\r') continue;
    if (c == '\n') { handleCommand(buf); buf = ""; continue; }
    if (buf.length() < 60) buf += c;
  }
}

// ============================================================================================

void setup() {
  iotStartSerial("Recon");
  iotInitI2C();
  iotInitSPI();
  iotInitDisplay();

  HCSR04.begin(26, 25); // 26 is trigger, 25 is echo
  mpuOk = mpu.begin(IoTPins::MPU6050_ADDR, &Wire);
  if (mpuOk) {
    mpu.setAccelerometerRange(MPU6050_RANGE_8_G);
    mpu.setGyroRange(MPU6050_RANGE_500_DEG);
    mpu.setFilterBandwidth(MPU6050_BAND_21_HZ);
    Serial.println("MPU6050: initialized");
  } else {
    Serial.println("MPU6050: not found at configured I2C address");
  }

  uint64_t chip = ESP.getEfuseMac();
  char idbuf[32];
  snprintf(idbuf, sizeof idbuf, "iot-%s-%08lX", TEAM_ID, (unsigned long)(chip & 0xFFFFFFFF));
  clientId = idbuf;

  // Startup window: 3 s to type PROVISION or FORGET.
  Serial.println("Type PROVISION (new credentials) or FORGET (erase) within 3 s...");
  iotDisplayLines("Recon cloud", "PROVISION/FORGET?", "(3 s, Serial)");
  String cmd = iotReadLine(3000);
  cmd.toUpperCase();
  if (cmd == "FORGET") forgetCredentials();

  bool haveCreds = loadCredentials();
  if (cmd == "PROVISION" || !haveCreds) {
    if (!haveCreds) Serial.println("No credentials in NVS yet.");
    iotDisplayLines("Recon cloud", "Provisioning:", "use Serial Monitor");
    runProvisioning();
  } else {
    Serial.println("Credentials loaded from NVS:");
    printCredentials();
  }

  WiFi.mode(WIFI_STA);
  configureMqtt();
  printHelp();
}

void loop() {
  pollConsole();
  maintainWiFi();
  maintainMqtt();
  mqtt.loop();   // EVERY pass: sends keepalive pings, reads incoming packets

  static uint32_t lastSensorPoll = 0;
  if (iotEvery(SENSOR_POLL_MS, lastSensorPoll)) {
    pollSensors();
  }

  static uint32_t lastEnvironmentPub = 0;
  if (iotEvery(ENVIRONMENT_PERIOD_MS, lastEnvironmentPub)) {
    if (mqtt.connected()) {
      if (publishEnvironment()) pubOk++; else pubFail++;
    }
  }

  static uint32_t lastDraw = 0;
  if (iotEvery(1000, lastDraw)) {
    const bool w = (WiFi.status() == WL_CONNECTED);
    String t = isfinite(distanceMm) ? String(distanceMm, 1) + " mm" : String("Distance unavailable");
    iotDisplayLines(String("Recon  ") + TEAM_ID,
                    w ? "WiFi " + String((int)WiFi.RSSI()) + " dBm" : String("WiFi: down"),
                    mqtt.connected() ? String("MQTT: online") : String("MQTT: retrying"),
                    t + (armed ? " armed" : " disarmed"));
  }
}
