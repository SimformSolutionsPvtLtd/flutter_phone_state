import 'dart:async';

import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_phone_state/extensions_static.dart';
import 'package:flutter_phone_state/phone_event.dart';
import 'package:url_launcher/url_launcher.dart';

export 'package:flutter_phone_state/phone_event.dart';

/// Phone events created by this plugin
final _localEvents = StreamController<PhoneCallEvent>.broadcast();
const MethodChannel _channel = MethodChannel('flutter_phone_state');

final _instance = FlutterPhoneState();

class FlutterPhoneState with WidgetsBindingObserver {
  FlutterPhoneState() {
    WidgetsBinding.instance.addObserver(this);
    _initializedNativeEvents?.forEach(_handleRawPhoneEvent);
  }
  static Future<String?> get platformVersion async {
    final version = await _channel.invokeMethod<String?>('getPlatformVersion');
    return version;
  }

  /// A broadcast stream of raw events from the underlying phone state.
  ///  It's preferred to use [phoneCallEvents]
  static Stream<RawPhoneEvent?>? get rawPhoneEvents => _initializedNativeEvents;

  /// A list of events associated to all calls.  This includes events
  /// from the underlying OS, as well as our
  /// own cancellation and timeout errors
  static Stream<PhoneCallEvent> get phoneCallEvents => _localEvents.stream;

  /// Places a phone call.  This will initiate a call on the target OS.
  /// The [PhoneCall] can be used to subscribe to events, or to await
  /// completion.  See see [PhoneCall.done] or [PhoneCall.eventStream]
  static PhoneCall startPhoneCall(String phoneNumber) {
    return _instance._makePhoneCall(phoneNumber);
  }

  /// Returns a list of active calls.
  static Iterable<PhoneCall> get activeCalls => [..._instance._calls.nonNulls];

  /// A list of active calls.  Theoretically, you could initiate a call
  /// while the first is still in flight.
  /// This should add both calls, and track them separately as best we can.
  ///
  /// As a note, Android does not support listening to events from nested calls.
  final _calls = <PhoneCall?>[];

  /// Finds a previously placed call that matches the incoming event
  PhoneCall? _findMatchingCall(RawPhoneEvent event) {
    // Either the first matching, or the first one without an ID
    PhoneCall? matching;
    if (event.id != null) {
      matching = firstOrNull(_calls, (c) => c?.callId == event.id);
    }
    matching ??=
        lastOrNull(_calls, (call) => call?.canBeLinked(event) ?? false);
    if (matching != null) {
      // Link them together for future reference
      matching.callId = event.id;
    }
    return matching;
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    debugPrint('Received application lifecycle state change: $state');

    if (state == AppLifecycleState.resumed) {
      /// We wait 1 second because ios has a short flash of resumed
      /// before the phone app opens
      Future.delayed(const Duration(seconds: 1), () {
        final expired = lastOrNull<PhoneCall?>(_calls, (PhoneCall? c) {
          return c != null &&
              c.status == PhoneCallStatus.dialing &&
              sinceNow(c.startTime).inSeconds < 30;
        });

        if (expired != null) {
          _changeStatus(expired, PhoneCallStatus.cancelled);
        }
      });
    }
  }

  Future<void> _openCallLink(PhoneCall call) async {
    /// Phone calls are weird in IOS.  We need to initiate the phone call
    /// by using the link below, but the app doesn't give us any meaningful
    /// feedback, so we mark the phone interaction as "complete"
    /// (technically this just means the call was started) by either (a)
    /// the applicationStateChange recognizing a return to the app (b) the
    /// event handler above fires with a call start event within 30 seconds
    /// (c) 5 seconds passes with no feedback (this will send back a result
    /// code of [cancelled], which means the call won't be logged
    try {
      final link = 'tel:${call.phoneNumber}';
      final status = await _openTelLink(link);

      if (status != LinkOpenResult.success) {
        _changeStatus(call, PhoneCallStatus.error);
        return;
      }

      /// If no activity reported within 10 seconds, we'll cancel the call
      await Future<void>.delayed(const Duration(seconds: 60));

      if (call.status == PhoneCallStatus.dialing) {
        _changeStatus(call, PhoneCallStatus.timedOut);
      }
    } catch (e) {
      _changeStatus(call, PhoneCallStatus.error);
    }
  }

  PhoneCall _makePhoneCall(String phoneNumber) {
    final call = PhoneCall.start(phoneNumber, PhoneCallPlacement.outbound);
    _calls.add(call);
    _changeStatus(call, PhoneCallStatus.dialing);
    _openCallLink(call);
    return call;
  }

  void _changeStatus(PhoneCall call, PhoneCallStatus status) {
    // create an event
    PhoneCallEvent event;
    if (call.events.any((e) => e.status == status)) {
      debugPrint('Call ${truncate(call.id, 8)} already has status $status');
    }
    if (status == PhoneCallStatus.disconnected ||
        status == PhoneCallStatus.timedOut ||
        status == PhoneCallStatus.error ||
        status == PhoneCallStatus.cancelled) {
      debugPrint('Call is done: ${call.id}- Removing due to $status');
      call.complete(status).then((event) {
        _localEvents.add(event);
      });
      _calls.removeWhere((existing) => existing == call);
    } else {
      event = call.recordStatus(status);
      _localEvents.add(event);
    }
  }

  Future<void> _handleRawPhoneEvent(RawPhoneEvent? event) async {
    if (event == null) return;
    try {
      _pruneCalls();
      var matching = _findMatchingCall(event);

      /// If no match was found?
      if (matching == null && event.isNewCall) {
        matching = PhoneCall.start(
          event.phoneNumber,
          event.type == RawEventType.inbound
              ? PhoneCallPlacement.inbound
              : PhoneCallPlacement.outbound,
          event.id,
        );
        _calls.add(matching);
        _changeStatus(
          matching,
          matching.isInbound
              ? PhoneCallStatus.ringing
              : PhoneCallStatus.dialing,
        );
        return;
      }

      if (matching == null) {
        // Nothing else we can do here...
        return;
      }
      switch (event.type) {
        case RawEventType.inbound:
          // Nothing
          break;
        case RawEventType.outbound:
          _changeStatus(matching, PhoneCallStatus.connecting);
        case RawEventType.connected:
          _changeStatus(matching, PhoneCallStatus.connected);
        case RawEventType.disconnected:

          /// We ended the call--- makes sure it's not
          /// some ridiculously long call
          _changeStatus(matching, PhoneCallStatus.disconnected);
      }
    } finally {}
  }

  /// Looks for calls that weren't properly terminated and completes them
  void _pruneCalls() {
    final expired = [..._calls.where((c) => c?.isExpired ?? false).nonNulls];
    for (final expiring in expired) {
      _changeStatus(expiring, PhoneCallStatus.timedOut);
    }
  }
}

/// The event channel to receive native phone events
const EventChannel _phoneStateCallEventChannel =
    EventChannel('co.sunnyapp/phone_events');

/// Native event stream, lazily created.  See [nativeEvents]
Stream<RawPhoneEvent?>? _nativeEvents;

/// A stream of [RawPhoneEvent] instances.  The stream only contains
/// null values if there was an error
Stream<RawPhoneEvent?>? get _initializedNativeEvents {
  return _nativeEvents ??= _phoneStateCallEventChannel
      .receiveBroadcastStream()
      .map<RawPhoneEvent?>((dyn) {
    try {
      if (dyn is! Map) return null;
      final event = dyn.cast<dynamic, dynamic>();
      final eventType = _parseEventType(event['type'] as String);
      return RawPhoneEvent(
        event['id'] as String,
        event['phoneNumber'] as String,
        eventType,
      );
    } catch (e) {
      return null;
    }
  });
}

RawEventType _parseEventType(String dyn) {
  switch (dyn) {
    case 'inbound':
      return RawEventType.inbound;
    case 'connected':
      return RawEventType.connected;
    case 'outbound':
      return RawEventType.outbound;
    case 'disconnected':
      return RawEventType.disconnected;
    default:
      throw 'Illegal raw event type: $dyn';
  }
}

/// Removes all non-numeric characters
String sanitizePhoneNumber(String input) {
  var out = '';

  for (var i = 0; i < input.length; ++i) {
    final char = input[i];
    if (_isNumeric(char)) {
      out += char;
    }
  }
  return out;
}

bool _isNumeric(String? str) {
  if (str == null) {
    return false;
  }
  return double.tryParse(str) != null;
}

Future<LinkOpenResult> _openTelLink(String? appLink) async {
  if (appLink == null) {
    return LinkOpenResult.invalidInput;
  }
  if (await canLaunchUrl(Uri.parse(appLink))) {
    return (await launchUrl(Uri.parse(appLink)))
        ? LinkOpenResult.success
        : LinkOpenResult.failed;
  } else {
    return LinkOpenResult.unsupported;
  }
}

enum LinkOpenResult { invalidInput, unsupported, success, failed }
