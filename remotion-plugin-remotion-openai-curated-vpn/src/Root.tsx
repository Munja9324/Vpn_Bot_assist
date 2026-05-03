import React from 'react';
import {Composition} from 'remotion';
import {VpnKbrLoader} from './VpnKbrLoader';

export const RemotionRoot: React.FC = () => {
  return (
    <Composition
      id="VpnKbrLoader"
      component={VpnKbrLoader}
      durationInFrames={150}
      fps={30}
      width={1080}
      height={1080}
    />
  );
};
