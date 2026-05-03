import React from 'react';
import {
  AbsoluteFill,
  Easing,
  interpolate,
  useCurrentFrame,
  useVideoConfig,
} from 'remotion';

const BG = '#f4f1ea';
const INK = '#151515';

const clamp = {
  extrapolateLeft: 'clamp' as const,
  extrapolateRight: 'clamp' as const,
};

const Arc: React.FC<{size: number; width: number; delay: number; opacity: number}> = ({
  size,
  width,
  delay,
  opacity,
}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const loop = ((frame - delay) % (2.4 * fps) + 2.4 * fps) % (2.4 * fps);
  const rotation = interpolate(loop, [0, 2.4 * fps], [0, 360]);
  const sweep = interpolate(loop, [0, 1.2 * fps, 2.4 * fps], [26, 112, 26], {
    ...clamp,
    easing: Easing.bezier(0.65, 0, 0.35, 1),
  });

  return (
    <div
      style={{
        position: 'absolute',
        width: size,
        height: size,
        borderRadius: '50%',
        border: `${width}px solid ${INK}`,
        opacity,
        transform: `rotate(${rotation}deg)`,
        clipPath: `polygon(50% 50%, 100% 0, 100% ${sweep}%, 0 100%, 0 0)`,
      }}
    />
  );
};

export const VpnKbrLoader: React.FC = () => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const intro = interpolate(frame, [0, 0.7 * fps], [0, 1], {
    ...clamp,
    easing: Easing.bezier(0.16, 1, 0.3, 1),
  });
  const progress = interpolate(frame, [0.2 * fps, 4.4 * fps], [0, 100], clamp);
  const caret = Math.floor(frame / 16) % 2 === 0 ? '_' : ' ';
  const pulse = interpolate(frame % fps, [0, fps / 2, fps], [0.32, 1, 0.32], clamp);

  return (
    <AbsoluteFill
      style={{
        background: BG,
        color: INK,
        alignItems: 'center',
        justifyContent: 'center',
        fontFamily:
          'IBM Plex Mono, SFMono-Regular, Consolas, Liberation Mono, Menlo, monospace',
      }}
    >
      <div
        style={{
          position: 'absolute',
          inset: 42,
          border: `1px solid ${INK}`,
          opacity: 0.16,
        }}
      />

      <div
        style={{
          position: 'relative',
          width: 430,
          height: 430,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          transform: `translateY(${interpolate(intro, [0, 1], [18, 0])}px)`,
          opacity: intro,
        }}
      >
        <Arc size={390} width={3} delay={0} opacity={0.92} />
        <Arc size={304} width={2} delay={18} opacity={0.4} />
        <div
          style={{
            width: 132,
            height: 132,
            border: `2px solid ${INK}`,
            borderRadius: 24,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
        >
          <div
            style={{
              width: 46,
              height: 58,
              border: `3px solid ${INK}`,
              borderRadius: '8px 8px 6px 6px',
              position: 'relative',
            }}
          >
            <div
              style={{
                position: 'absolute',
                left: 7,
                top: -26,
                width: 26,
                height: 28,
                border: `3px solid ${INK}`,
                borderBottom: 0,
                borderRadius: '18px 18px 0 0',
              }}
            />
            <div
              style={{
                position: 'absolute',
                left: 18,
                top: 22,
                width: 8,
                height: 8,
                borderRadius: '50%',
                background: INK,
                opacity: pulse,
              }}
            />
          </div>
        </div>
      </div>

      <div
        style={{
          position: 'absolute',
          bottom: 152,
          width: 520,
          maxWidth: '72%',
        }}
      >
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'baseline',
            fontSize: 28,
            fontWeight: 600,
            letterSpacing: 0,
          }}
        >
          <span>vpn_kbr{caret}</span>
          <span>{Math.round(progress).toString().padStart(3, '0')}%</span>
        </div>
        <div
          style={{
            marginTop: 24,
            height: 2,
            width: '100%',
            background: `${INK}22`,
            overflow: 'hidden',
          }}
        >
          <div
            style={{
              width: `${progress}%`,
              height: '100%',
              background: INK,
            }}
          />
        </div>
        <div
          style={{
            marginTop: 18,
            fontSize: 16,
            textTransform: 'uppercase',
            opacity: 0.54,
            letterSpacing: 0,
          }}
        >
          establishing secure route
        </div>
      </div>
    </AbsoluteFill>
  );
};
